/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.lwdataprovider.db

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Order
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventFilter
import com.exactpro.cradle.testevents.TestEventFilterBuilder
import com.exactpro.th2.lwdataprovider.db.util.asIterableWithMeasurements
import com.exactpro.th2.lwdataprovider.db.util.getGenericWithSyncInterval
import com.exactpro.th2.lwdataprovider.db.util.withMeasurements
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.producers.EventProducer
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.Collections
import kotlin.system.measureTimeMillis


class CradleEventExtractor(
    cradleManager: CradleManager,
    private val dataMeasurement: DataMeasurement,
) {
    private val storage: CradleStorage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun getAllEventsScopes(bookId: BookId): Set<String> {
        return measure("scopes") { storage.getScopes(bookId) }.toSet()
    }

    fun getScopes(bookId: BookId, start: Instant, end: Instant): Iterator<String> {
        return storage.getScopes(bookId, Interval(start, end))
    }

    fun getEvents(filter: SseEventSearchRequest, sink: EventDataSink<Event>) {
        val commonFilterSupplier: (start: Instant, end: Instant?) -> TestEventFilterBuilder = { start, end ->
            TestEventFilter.builder()
                .apply {
                    when (filter.searchDirection) {
                        SearchDirection.next -> {
                            startTimestampFrom().isGreaterThanOrEqualTo(start)
                            end?.also { startTimestampTo().isLessThan(it) }
                        }

                        SearchDirection.previous -> {
                            startTimestampTo().isLessThanOrEqualTo(start)
                            end?.also { startTimestampFrom().isGreaterThan(it) }
                        }
                    }
                }
                .order(
                    when (filter.searchDirection) {
                        SearchDirection.previous -> Order.REVERSE
                        SearchDirection.next -> Order.DIRECT
                    }
                )
                .bookId(filter.bookId)
                .scope(filter.scope)
        }

        if (filter.parentEvent == null) {
            getEventByDates(filter.startTimestamp, filter.endTimestamp, sink, filter.filter) { start, end ->
                logger.info { "Extracting events from $start to $end processed." }
                commonFilterSupplier(start, end).build()
            }
        } else {
            val parentId: StoredTestEventId = filter.parentEvent.eventId
            getEventByDates(filter.startTimestamp, filter.endTimestamp, sink, filter.filter) { start, end ->
                logger.info { "Extracting events from $start to $end with parent $parentId processed." }
                commonFilterSupplier(start, end)
                    .parent(parentId)
                    .build()
            }
        }
    }

    fun getSingleEvents(filter: GetEventRequest, sink: EventDataSink<Event>) {
        logger.info { "Extracting single event $filter" }
        val batchId = filter.batchId
        val eventId = StoredTestEventId.fromString(filter.eventId)
        if (batchId != null) {
            val testBatch = measure("single_event") { storage.getTestEvent(StoredTestEventId.fromString(batchId)) }
            if (testBatch == null) {
                sink.onError("Event batch is not found with id: '$batchId'", batchId = batchId)
                return
            }
            if (testBatch.isSingle) {
                sink.onError("Event with id: '$batchId' is not a batch. (single event)", id = batchId)
                return
            }
            val batch = testBatch.asBatch()
            val testEvent = batch.getTestEvent(eventId)
            if (testEvent == null) {
                sink.onError("Event with id: '$eventId' is not found in batch '$batchId'", filter.eventId, batchId)
                return
            }
            val batchEventBody = EventProducer.fromBatchEvent(testEvent, batch)

            sink.onNext(batchEventBody.convertToEvent())
        } else {
            val testBatch = measure("single_event") { storage.getTestEvent(eventId) }
            if (testBatch == null) {
                sink.onError("Event is not found with id: '$eventId'", filter.eventId)
                return
            }
            if (testBatch.isBatch) {
                sink.onError("Event with id: '$eventId' is a batch. (not single event)", filter.eventId)
                return
            }
            processEvents(Collections.singleton(testBatch), sink, ProcessingInfo(), DataFilter.acceptAll())
        }
    }

    fun getEventsWithSyncInterval(
        startTimestamp: Instant,
        endTimestamp: Instant,
        syncInterval: Duration,
        scopesByBook: Map<BookId, Set<String>>,
        sink: EventDataSink<Event>,
    ) {
        data class BookScope(val bookId: BookId, val scope: String)

        val stat = ProcessingInfo()
        val timeMillis = measureTimeMillis {
            getGenericWithSyncInterval(
                logger,
                scopesByBook.asSequence().flatMap { (bookId, scopes) -> scopes.map { BookScope(bookId, it) } }.toList(),
                syncInterval,
                sink,
                { processTestEvent(it, stat, DataFilter.acceptAll(), sink) },
                { testEvent ->
                    testEvent.run {
                        if (isBatch) {
                            asBatch().testEvents.minOf { it.startTimestamp }
                        } else {
                            asSingle().startTimestamp
                        }
                    }
                }
            ) { (bookId, scope) ->
                storage.getTestEvents(
                    TestEventFilter.builder()
                        .bookId(bookId)
                        .scope(scope)
                        .startTimestampFrom().isGreaterThanOrEqualTo(startTimestamp)
                        .startTimestampTo().isLessThan(endTimestamp)
                        .build()
                ).withMeasurements("event", dataMeasurement)
            }
        }
        logger.info { "Loaded events $stat in ${Duration.ofMillis(timeMillis)}" }
    }

    private fun getEventByDates(
        startTimestamp: Instant,
        endTimestamp: Instant?,
        sink: EventDataSink<Event>,
        filter: DataFilter<BaseEventEntity>,
        filterSupplier: (Instant, Instant?) -> TestEventFilter,
    ) {
        val counter = ProcessingInfo()
        val startTime = System.currentTimeMillis()
        val cradleFilter = filterSupplier(startTimestamp, endTimestamp)
        val order = requireNotNull(cradleFilter.order) { "order is null" }
        fun compareStart(event: BaseEventEntity): Boolean {
            return when (order) {
                Order.DIRECT -> event.startTimestamp >= startTimestamp
                Order.REVERSE -> event.startTimestamp <= startTimestamp
            }
        }

        fun compareEnd(event: BaseEventEntity): Boolean {
            if (endTimestamp == null) return true
            return when (order) {
                Order.DIRECT -> event.startTimestamp < endTimestamp
                Order.REVERSE -> event.startTimestamp > endTimestamp
            }
        }

        val testEvents = measure("init_request") { storage.getTestEvents(cradleFilter) }
        processEvents(testEvents.asIterableWithMeasurements("event", dataMeasurement), sink, counter) { event ->
            compareStart(event) && compareEnd(event)
                    && filter.match(event)
        }
        logger.info { "Events for this period loaded. Count: $counter. Time ${System.currentTimeMillis() - startTime} ms" }
        sink.canceled?.apply {
            logger.info { "Loading events stopped: $message" }
            return
        }
    }

    private fun processEvents(
        testEvents: Iterable<StoredTestEvent>,
        sink: EventDataSink<Event>,
        count: ProcessingInfo,
        filter: DataFilter<BaseEventEntity>,
    ) {
        for (testEvent in testEvents) {
            processTestEvent(testEvent, count, filter, sink)
            sink.canceled?.apply {
                logger.info { "events processing canceled: $message" }
                return
            }
        }
    }

    private fun processTestEvent(
        testEvent: StoredTestEvent,
        count: ProcessingInfo,
        filter: DataFilter<BaseEventEntity>,
        sink: EventDataSink<Event>
    ) {
        if (testEvent.isSingle) {
            val singleEv = testEvent.asSingle()
            val event = EventProducer.fromSingleEvent(singleEv)
            count.total++
            if (!filter.match(event)) {
                return
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
    }

    private inline fun <T> measure(name: String, action: () -> T): T = dataMeasurement.start(name).use { action() }
}

data class ProcessingInfo(
    var total: Long = 0,
    var events: Long = 0,
    var singleEvents: Long = 0,
    var batches: Long = 0,
    var totalContentSize: Long = 0,
)
