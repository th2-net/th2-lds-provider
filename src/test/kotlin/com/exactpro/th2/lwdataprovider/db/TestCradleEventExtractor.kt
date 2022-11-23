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

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.testevents.StoredTestEvent
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventWrapper
import com.exactpro.cradle.testevents.TestEventBatchToStore
import com.exactpro.cradle.testevents.TestEventToStore
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.util.createEventToStore
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
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
        val toStore = createEventToStore(eventId = "test", start, end)
        doReturn(
            listOf(
                StoredTestEventWrapper(
                    StoredTestEvent.newStoredTestEventSingle(toStore)
                )
            )
        ).whenever(storage).getTestEvents(eq(start), eq(end))

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
        val toStore = createEventToStore("test", start, end, parentEventId = StoredTestEventId("batchParent"))
        doReturn(
            listOf(
                StoredTestEventWrapper(
                    StoredTestEvent.newStoredTestEventBatch(TestEventBatchToStore.builder()
                        .id(StoredTestEventId("batch"))
                        .parentId(StoredTestEventId("batchParent"))
                        .build())
                        .apply { addTestEvent(toStore) }
                )
            )
        ).whenever(storage).getTestEvents(eq(start), eq(end))

        val sink: EventDataSink<Event> = mock { }
        extractor.getEvents(createRequest(start, end), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore, StoredTestEventId("batch"))
    }

    @Test
    fun `splits requests if more than one day covered`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-15T23:59:59.999999999Z")
        val middle = end.minus(1, ChronoUnit.DAYS)
        val firstToStore = createEventToStore(eventId = "first", start, middle)
        doReturn(
            listOf(
                StoredTestEventWrapper(
                    StoredTestEvent.newStoredTestEventSingle(firstToStore)
                )
            )
        ).whenever(storage).getTestEvents(eq(start), eq(middle))
        val secondToStore = createEventToStore(eventId = "second", middle, end)
        doReturn(
            listOf(
                StoredTestEventWrapper(
                    StoredTestEvent.newStoredTestEventSingle(secondToStore)
                )
            )
        ).whenever(storage).getTestEvents(eq(start.plus(1, ChronoUnit.DAYS)), eq(end))

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
        val toStore = createEventToStore(eventId = "test", start, end)
        doReturn(
            StoredTestEventWrapper(
                StoredTestEvent.newStoredTestEventSingle(toStore)
            )
        ).whenever(storage).getTestEvent(eq(StoredTestEventId("test")))

        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest(null, "test"), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore)
    }

    @Test
    fun `returns single event from batch`() {
        val start = Instant.parse("2022-11-14T00:00:00Z")
        val end = Instant.parse("2022-11-14T23:59:59.999999999Z")
        val toStore = createEventToStore(eventId = "test", start, end, parentEventId = StoredTestEventId("batchParent"))
        doReturn(
            StoredTestEventWrapper(
                StoredTestEvent.newStoredTestEventBatch(TestEventBatchToStore.builder()
                    .id(StoredTestEventId("batch"))
                    .parentId(StoredTestEventId("batchParent"))
                    .build())
                    .apply { addTestEvent(toStore) }
            )
        ).whenever(storage).getTestEvent(eq(StoredTestEventId("batch")))

        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest("batch", "test"), sink)
        val event = argumentCaptor<Event>()
        verify(sink).onNext(event.capture())
        expectThat(event.lastValue).isEqualTo(toStore, batchId = StoredTestEventId("batch"))
    }

    @Test
    fun `reports unknown event`() {
        val sink: EventDataSink<Event> = mock { }
        extractor.getSingleEvents(GetEventRequest(null, "test"), sink)
        val event = argumentCaptor<Event>()
        verify(sink, never()).onNext(event.capture())
        verify(sink).onError(eq("Event is not found with id: test"))
    }

    private fun Assertion.Builder<Event>.isEqualTo(toStore: TestEventToStore, batchId: StoredTestEventId? = null) {
        get { eventId } isEqualTo (batchId?.let { "${it.id}:${toStore.id.id}" } ?: toStore.id.id)
        get { parentEventId } isEqualTo toStore.parentId?.id
        get { eventName } isEqualTo toStore.name
        get { eventType } isEqualTo toStore.type
        get { successful } isEqualTo toStore.isSuccess
        get { body } isEqualTo (toStore.content.takeIf { it.isNotEmpty() }?.toString(Charsets.UTF_8) ?: "{}")
    }

    private fun createRequest(start: Instant?, end: Instant?): SseEventSearchRequest = SseEventSearchRequest(
        startTimestamp = start,
        endTimestamp = end,
        parentEvent = null,
        resultCountLimit = 0,
        searchDirection = TimeRelation.AFTER,
    )
}