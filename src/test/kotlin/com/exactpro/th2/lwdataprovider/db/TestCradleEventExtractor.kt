/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.testevents.StoredTestEventBatch
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventToStore
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.util.ListCradleResult
import com.exactpro.th2.lwdataprovider.util.createEventId
import com.exactpro.th2.lwdataprovider.util.createEventStoredEvent
import com.exactpro.th2.lwdataprovider.util.toStoredEvent
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.isNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.get
import strikt.assertions.isEqualTo
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class TestCradleEventExtractor {
    private val storage = mock<CradleStorage>()
    private val manager = mock<CradleManager> {
        on { storage } doReturn storage
    }
    private val extractor = CradleEventExtractor(manager)

    @Test
    fun `returns events for single day`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val toStore = createEventStoredEvent(eventId = "test", start, end)
        doReturn(
            ListCradleResult(arrayListOf(toStore.toStoredEvent()))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == start && startTimestampTo.value == end
        })

        val sink: EventDataSink<Event> = mock { }
        extractor.getEvents(createRequest(start, end), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore)
    }

    @Test
    fun `returns batched events for single day`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val toStore = createEventStoredEvent("test", start, end, parentEventId = createEventId("batchParent", timestamp = start))
        val batchId = createEventId("batch", timestamp = start)
        doReturn(
            ListCradleResult(arrayListOf(
                TestEventToStore.batchBuilder(10_000)
                    .id(batchId)
                    .parentId(createEventId("batchParent", timestamp = start))
                    .build().apply {
                        addTestEvent(toStore)
                    }.let {
                        StoredTestEventBatch(it, null)
                    }
            ))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == start && startTimestampTo.value == end
        })

        val sink: EventDataSink<Event> = mock { }
        extractor.getEvents(createRequest(start, end), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore, batchId)
    }

    @Test
    fun `filters events from batch if start timestamp is not in the range`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val outRange = createEventStoredEvent("test", start.minusSeconds(20), end, parentEventId = createEventId("batchParent", timestamp = start))
        val inRange = createEventStoredEvent("test", start.plusSeconds(20), end, parentEventId = createEventId("batchParent", timestamp = start))
        val batchId = createEventId("batch", timestamp = start.minusSeconds(60))
        doReturn(
            ListCradleResult(arrayListOf(
                TestEventToStore.batchBuilder(10_000)
                    .id(batchId)
                    .parentId(createEventId("batchParent", timestamp = start))
                    .build().apply {
                        addTestEvent(outRange)
                        addTestEvent(inRange)
                    }.let {
                        StoredTestEventBatch(it, null)
                    }
            ))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == start && startTimestampTo.value == end
        })

        val sink: EventDataSink<Event> = mock { }
        extractor.getEvents(createRequest(start, end), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(inRange, batchId)
    }

    @Test
    fun `splits requests if more than one day covered`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-15T23:59:59.999999999Z")
        val middle = end.minus(1, ChronoUnit.DAYS)

        val firstToStore = createEventStoredEvent(eventId = "first", start, middle)
        val secondToStore = createEventStoredEvent(eventId = "second", middle, end)

        doReturn(
            ListCradleResult(arrayListOf(firstToStore.toStoredEvent(), secondToStore.toStoredEvent()))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == start && startTimestampTo.value == end
        })

        val sink: EventDataSink<Event> = mock { }
        extractor.getEvents(createRequest(start, end), sink)
        val event = argumentCaptor<Event>()
        verify(sink, times(2)).onNext(event.capture())
        expectThat(event.allValues) {
            get(0).isEqualTo(firstToStore)
            get(1).isEqualTo(secondToStore)
        }
    }

    @Test
    fun `returns single event`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val toStore = createEventStoredEvent(eventId = "test", start, end)
        val eventId = createEventId("test")
        doReturn(
            toStore.toStoredEvent()
        ).whenever(storage).getTestEvent(eq(eventId))

        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest(null, eventId.toString()), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore)
    }

    @Test
    fun `returns single event from batch`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val toStore = createEventStoredEvent(eventId = "test", start, end, parentEventId = createEventId("batchParent", timestamp = start))
        val batchId = createEventId("batch", timestamp = start)
        doReturn(
            TestEventToStore.batchBuilder(10_000)
                .id(batchId)
                .parentId(createEventId("batchParent", timestamp = start))
                .build().apply {
                    addTestEvent(toStore)
                }.let {
                    StoredTestEventBatch(it, null)
                }

        ).whenever(storage).getTestEvent(eq(batchId))

        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest(batchId.toString(), toStore.id.toString()), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore, batchId)
    }

    @Test
    fun `reports unknown event`() {
        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest(null, createEventId("test", timestamp = Instant.ofEpochSecond(1)).toString()), sink)
        val event = argumentCaptor<Event>()
        verify(sink, never()).onNext(event.capture())
        verify(sink).onError(
            eq("Event is not found with id: 'test:test-scope:19700101000001000000000:test'"),
            eq("test:test-scope:19700101000001000000000:test"),
            isNull()
        )
    }

    private fun Assertion.Builder<Event>.isEqualTo(toStore: TestEventToStore, batchId: StoredTestEventId? = null) {
        get { eventId } isEqualTo (batchId?.let { "${it}>${toStore.id}" } ?: toStore.id.toString())
        get { parentEventId } isEqualTo toStore.parentId?.let { ProviderEventId(null, it) }
        get { eventName } isEqualTo toStore.name
        get { eventType } isEqualTo toStore.type
        get { successful } isEqualTo toStore.isSuccess
        get { body } isEqualTo (toStore.asSingle().content.takeIf { it.isNotEmpty() }?.toString(Charsets.UTF_8) ?: "[]")
    }

    private fun createRequest(start: Instant?, end: Instant?): SseEventSearchRequest = SseEventSearchRequest(
        startTimestamp = start,
        endTimestamp = end,
        parentEvent = null,
        resultCountLimit = 0,
        searchDirection = SearchDirection.next,
        bookId = BookId("test"),
        scope = "test-scope",
    )
}