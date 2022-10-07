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
import com.exactpro.cradle.cassandra.CassandraCradleStorage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.http.EventRequestContext
import com.exactpro.th2.lwdataprovider.producers.EventProducer
import mu.KotlinLogging
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.stream.Collectors


class CradleEventExtractor (private val cradleManager: CradleManager) {

    private val storage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun getEvents(filter: SseEventSearchRequest, requestContext: EventRequestContext) {
        val dates = splitByDates(filter.startTimestamp, filter.endTimestamp)
        if (filter.resultCountLimit != null && filter.resultCountLimit > 0) {
            requestContext.eventsLimit = filter.resultCountLimit
        }

        if (filter.parentEvent == null) {
            getEventByDates(dates, requestContext, filter)
        } else {
            getEventByIds(filter.parentEvent, dates, requestContext, filter)
        }
        requestContext.finishStream()
    }

    fun getSingleEvents(filter: GetEventRequest, requestContext: EventRequestContext) {
        val batchId = filter.batchId
        val eventId = StoredTestEventId(filter.eventId)
        if (batchId != null) {
            val testBatch = storage.getTestEvent(StoredTestEventId(batchId))
            if (testBatch == null) {
                requestContext.writeErrorMessage("Event batch is not found with id: $batchId")
                requestContext.finishStream()
                return
            }
            if (testBatch.isSingle) {
                requestContext.writeErrorMessage("Event with id: $batchId is not a batch. (single event)")
                requestContext.finishStream()
                return
            }
            val batch = testBatch.asBatch()
            val testEvent = batch.getTestEvent(eventId)
            if (testEvent == null) {
                requestContext.writeErrorMessage("Event with id: $eventId is not found in batch $batchId")
                requestContext.finishStream()
                return
            }
            val batchEventBody = EventProducer.fromBatchEvent(testEvent, batch)
            batchEventBody.body = String(testEvent.content)
            batchEventBody.attachedMessageIds = loadAttachedMessages(testEvent.messageIds)

            requestContext.processEvent(batchEventBody.convertToEvent())
        } else {
            val testBatch = storage.getTestEvent(eventId)
            if (testBatch == null) {
                requestContext.writeErrorMessage("Event is not found with id: $eventId")
                requestContext.finishStream()
                return
            }
            if (testBatch.isBatch) {
                requestContext.writeErrorMessage("Event with id: $eventId is a batch. (not single event)")
                requestContext.finishStream()
                return
            }
            processEvents(Collections.singleton(testBatch), requestContext, ProcessingInfo(), DataFilter.acceptAll())
        }
        requestContext.finishStream()
    }

    private fun toLocal(timestamp: Instant?): LocalDateTime {
        return LocalDateTime.ofInstant(timestamp, CassandraCradleStorage.TIMEZONE_OFFSET)
    }

    private fun toInstant(timestamp: LocalDateTime): Instant {
        return timestamp.toInstant(CassandraCradleStorage.TIMEZONE_OFFSET)
    }


    private fun splitByDates(from: Instant?, to: Instant?): Collection<Pair<Instant, Instant>> {
        checkNotNull(from)
        checkNotNull(to)
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

    private fun getEventByDates(dates: Collection<Pair<Instant, Instant>>, requestContext: EventRequestContext, request: SseEventSearchRequest) {
        for (splitByDate in dates) {
            val counter = ProcessingInfo()
            val startTime = System.currentTimeMillis()
            logger.info { "Extracting events from ${splitByDate.first} to ${splitByDate.second} processed."}
            val testEvents = storage.getTestEvents(splitByDate.first, splitByDate.second)
            processEvents(testEvents, requestContext, counter, request.filter)
            logger.info { "Events for this period loaded. Info: ${counter.toShortString()}. Time ${System.currentTimeMillis() - startTime} ms"}
            if (requestContext.isLimitReached()) {
                logger.info { "Loading events stopped: Reached events limit" }
                break
            }
            if (!requestContext.contextAlive) {
                logger.info { "Loading events stopped: Context was killed" }
                break
            }
        }
    }

    private fun getEventByIds(
        id: ProviderEventId,
        dates: Collection<Pair<Instant, Instant>>,
        requestContext: EventRequestContext,
        request: SseEventSearchRequest
    ) {
        for (splitByDate in dates) {
            val counter = ProcessingInfo()
            val startTime = System.currentTimeMillis()
            logger.info { "Extracting events from ${splitByDate.first} to ${splitByDate.second} with parent ${id.eventId} processed."}
            val testEvents = storage.getTestEvents(id.eventId, splitByDate.first, splitByDate.second)
            processEvents(testEvents, requestContext, counter, request.filter)
            logger.info { "Events for this period loaded. Info: ${counter.toShortString()}. Time ${System.currentTimeMillis() - startTime} ms"}
            if (requestContext.isLimitReached()) {
                logger.info { "Loading events stopped: Reached events limit" }
                break
            }
            if (!requestContext.contextAlive) {
                logger.info { "Loading events stopped: Context was killed" }
                break
            }
        }
    }

    private fun ProcessingInfo.toShortString(): String {
        return "events: $events, batches: $batches, total content size (KB): ${totalContentSize / 1024}"
    }
    
    private fun loadAttachedMessages(messageIds: Collection<StoredMessageId>?): Set<String> {
        return if (messageIds != null) {
            messageIds.stream().map { t -> t.toString() }.collect(Collectors.toSet())
        } else {
            Collections.emptySet()
        }
    }
    
    private fun processEvents(
        testEvents: Iterable<StoredTestEventWrapper>,
        requestContext: EventRequestContext,
        count: ProcessingInfo,
        filter: DataFilter<BaseEventEntity>,
    ) {
        for (testEvent in testEvents) {
            if (testEvent.isSingle) {
                val singleEv = testEvent.asSingle()
                val event = EventProducer.fromSingleEvent(singleEv)
                event.body = String(singleEv.content)
                event.attachedMessageIds = loadAttachedMessages(singleEv.messageIds)
                if (!filter.match(event)) {
                    continue
                }
                count.events++
                count.totalContentSize += singleEv.content.size + event.attachedMessageIds.sumOf { it.length }
                requestContext.processEvent(event.convertToEvent())
                requestContext.addProcessedEvents(1)
            } else if (testEvent.isBatch) {
                count.batches++
                val batch = testEvent.asBatch()
                val eventsList = batch.testEvents
                for (batchEvent in eventsList) {
                    val batchEventBody = EventProducer.fromBatchEvent(batchEvent, batch)
                    batchEventBody.body = String(batchEvent.content)
                    batchEventBody.attachedMessageIds = loadAttachedMessages(batchEvent.messageIds)
                    if (!filter.match(batchEventBody)) {
                        continue
                    }

                    count.events++
                    count.totalContentSize += batchEvent.content.size + batchEventBody.attachedMessageIds.sumOf { it.length }
                    requestContext.processEvent(batchEventBody.convertToEvent())
                }
                requestContext.addProcessedEvents(eventsList.size)
            }
            if (requestContext.isLimitReached() || !requestContext.contextAlive) {
                return
            }
        }
    }
}

class ProcessingInfo(
    var events: Long = 0,
    var batches: Long = 0,
    var totalContentSize: Long = 0,
)