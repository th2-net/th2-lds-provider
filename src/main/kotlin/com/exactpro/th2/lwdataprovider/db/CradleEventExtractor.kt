/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.db

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.cassandra.CassandraCradleStorage
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.producers.EventProducer
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit
import java.util.*


class CradleEventExtractor(
    cradleManager: CradleManager
) {
    private val storage: CradleStorage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun getEvents(filter: SseEventSearchRequest, sink: EventDataSink<Event>) {
        val dates = splitByDates(
            requireNotNull(filter.startTimestamp) { "start timestamp is not set" },
            requireNotNull(filter.endTimestamp) { "end timestamp is not set" }
        )

        if (filter.parentEvent == null) {
            getEventByDates(dates, sink, filter.filter) { start, end ->
                logger.info { "Extracting events from $start to $end processed." }
                storage.getTestEvents(start, end)
            }
        } else {
            val parentId: StoredTestEventId = filter.parentEvent.eventId
            getEventByDates(dates, sink, filter.filter) { start, end ->
                logger.info { "Extracting events from $start to $end with parent $parentId processed." }
                storage.getTestEvents(parentId, start, end)
            }
        }
    }

    fun getSingleEvents(filter: GetEventRequest, sink: EventDataSink<Event>) {
        logger.info { "Extracting single event $filter" }
        val batchId = filter.batchId
        val eventId = StoredTestEventId(filter.eventId)
        if (batchId != null) {
            val testBatch = storage.getTestEvent(StoredTestEventId(batchId))
            if (testBatch == null) {
                sink.onError("Event batch is not found with id: $batchId")
                return
            }
            if (testBatch.isSingle) {
                sink.onError("Event with id: $batchId is not a batch. (single event)")
                return
            }
            val batch = testBatch.asBatch()
            val testEvent = batch.getTestEvent(eventId)
            if (testEvent == null) {
                sink.onError("Event with id: $eventId is not found in batch $batchId")
                return
            }
            val batchEventBody = EventProducer.fromBatchEvent(testEvent, batch)

            sink.onNext(batchEventBody.convertToEvent())
        } else {
            val testBatch = storage.getTestEvent(eventId)
            if (testBatch == null) {
                sink.onError("Event is not found with id: $eventId")
                return
            }
            if (testBatch.isBatch) {
                sink.onError("Event with id: $eventId is a batch. (not single event)")
                return
            }
            processEvents(Collections.singleton(testBatch), sink, ProcessingInfo(), DataFilter.acceptAll())
        }
    }

    private fun toLocal(timestamp: Instant?): LocalDateTime {
        return LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET)
    }

    private fun toInstant(timestamp: LocalDateTime): Instant {
        return timestamp.toInstant(CassandraCradleStorage.TIMEZONE_OFFSET)
    }


    private fun splitByDates(from: Instant, to: Instant): Collection<Pair<Instant, Instant>> {
        require(!from.isAfter(to)) { "Lower boundary should specify timestamp before upper boundary, but got $from > $to" }
        var localFrom: LocalDateTime = toLocal(from)
        val localTo: LocalDateTime = toLocal(to)
        val result: MutableCollection<Pair<Instant, Instant>> = ArrayList()
        do {
            if (localFrom.toLocalDate() == localTo.toLocalDate()) {
                result.add(toInstant(localFrom) to toInstant(localTo))
                return result
            }
            val eod = localFrom.toLocalDate().atTime(LocalTime.MAX)
            result.add(toInstant(localFrom) to toInstant(eod))
            localFrom = eod.plus(1, ChronoUnit.NANOS)
        } while (true)
    }

    private fun getEventByDates(
        dates: Collection<Pair<Instant, Instant>>,
        sink: EventDataSink<Event>,
        filter: DataFilter<BaseEventEntity>,
        eventSupplier: (Instant, Instant) -> Iterable<StoredTestEventWrapper>,
    ) {
        for (splitByDate in dates) {
            val counter = ProcessingInfo()
            val startTime = System.currentTimeMillis()
            val testEvents = eventSupplier(splitByDate.first, splitByDate.second)
            processEvents(testEvents, sink, counter, filter)
            logger.info { "Events for this period loaded. Count: $counter. Time ${System.currentTimeMillis() - startTime} ms" }
            sink.canceled?.apply {
                logger.info { "Loading events stopped: $message" }
                return
            }
        }
    }

    private fun processEvents(
        testEvents: Iterable<StoredTestEventWrapper>,
        sink: EventDataSink<Event>,
        count: ProcessingInfo,
        filter: DataFilter<BaseEventEntity>,
    ) {
        for (testEvent in testEvents) {
            if (testEvent.isSingle) {
                val singleEv = testEvent.asSingle()
                val event = EventProducer.fromSingleEvent(singleEv)
                count.total++;
                if (!filter.match(event)) {
                    continue
                }
                count.singleEvents++
                count.events++
                count.totalContentSize += singleEv.content.size + event.attachedMessageIds.sumOf { it.length }
                sink.onNext(event.convertToEvent())
            } else if (testEvent.isBatch) {
                count.batches++
                val batch = testEvent.asBatch()
                val eventsList = batch.testEvents
                for (batchEvent in eventsList) {
                    val batchEventBody = EventProducer.fromBatchEvent(batchEvent, batch)
                    count.total++
                    if (!filter.match(batchEventBody)) {
                        continue
                    }

                    count.events++
                    count.totalContentSize += batchEvent.content.size + batchEventBody.attachedMessageIds.sumOf { it.length }
                    sink.onNext(batchEventBody.convertToEvent())
                }
            }
            sink.canceled?.apply {
                logger.info { "events processing canceled: $message" }
                return
            }
        }
    }
}

class ProcessingInfo(
    var total: Long = 0,
    var events: Long = 0,
    var singleEvents: Long = 0,
    var batches: Long = 0,
    var totalContentSize: Long = 0,
)
