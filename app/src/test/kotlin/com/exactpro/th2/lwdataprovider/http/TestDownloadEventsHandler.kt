/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.testevents.StoredTestEventIdUtils
import com.exactpro.cradle.utils.CradleStorageException
import com.exactpro.th2.lwdataprovider.util.CradleResult
import com.exactpro.th2.lwdataprovider.util.createEventId
import com.exactpro.th2.lwdataprovider.util.createEventStoredEvent
import com.exactpro.th2.lwdataprovider.util.toStoredEvent
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.time.Instant

class TestDownloadEventsHandler : AbstractHttpHandlerTest<DownloadEventsHandler>() {
    override fun createHandler(): DownloadEventsHandler {
        return DownloadEventsHandler(
            configuration,
            convExecutor = context.convExecutor,
            sseResponseBuilder,
            keepAliveHandler = context.keepAliveHandler,
            searchEventsHandler = context.searchEventsHandler,
            context.requestsDataMeasurement,
        )
    }


    @Test
    fun `response with events`() {
        val start = Instant.now().minusSeconds(100)
        val end = start.plusSeconds(5)
        val parentEventId = createEventId("-1", "test-book", "test-scope", start)
        doReturn(
            CradleResult(
                *buildList {
                    repeat(6) { index ->
                        add(
                            createEventStoredEvent(
                                index.toString(),
                                start,
                                end,
                                parentEventId,
                                "test-scope",
                                "test-book",
                            ).toStoredEvent()
                        )
                    }
                }.toTypedArray()
            )
        ).whenever(storage).getTestEvents(argThat {
            scope == "test-scope" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.get(
                "/download/events?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&scope=test-scope" +
                        "&bookId=test-book"
            )

            val startSeconds = start.epochSecond
            val startNanos = start.nano
            val endSeconds = end.epochSecond
            val endNanos = end.nano
            val cradleTime = StoredTestEventIdUtils.timestampToString(start)
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"eventId":"test-book:test-scope:$cradleTime:0","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:1","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:2","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:3","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:4","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:5","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `response with events with limit`() {
        val start = Instant.now().minusSeconds(100)
        val end = start.plusSeconds(5)
        val parentEventId = createEventId("-1", "test-book", "test-scope", start)
        doReturn(
            CradleResult(
                *buildList {
                    repeat(6) { index ->
                        add(
                            createEventStoredEvent(
                                index.toString(),
                                start,
                                end,
                                parentEventId,
                                "test-scope",
                                "test-book",
                            ).toStoredEvent()
                        )
                    }
                }.toTypedArray()
            )
        ).whenever(storage).getTestEvents(argThat {
            scope == "test-scope" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.get(
                "/download/events?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&scope=test-scope" +
                        "&bookId=test-book" +
                        "&limit=4"
            )

            val startSeconds = start.epochSecond
            val startNanos = start.nano
            val endSeconds = end.epochSecond
            val endNanos = end.nano
            val cradleTime = StoredTestEventIdUtils.timestampToString(start)
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"eventId":"test-book:test-scope:$cradleTime:0","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:1","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:2","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |{"eventId":"test-book:test-scope:$cradleTime:3","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":$endSeconds,"nano":$endNanos},"startTimestamp":{"epochSecond":$startSeconds,"nano":$startNanos},"parentEventId":"test-book:test-scope:$cradleTime:-1","successful":true,"bookId":"test-book","scope":"test-scope","attachedMessageIds":[],"body":[]}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `respond with error and correct status if first cradle call throws an exception`() {
        whenever(storage.getTestEvents(any())) doThrow CradleStorageException("ignore")

        startTest { _, client ->
            val now = Instant.now().toEpochMilli()
            val response = client.get(
                "/download/events?" +
                        "startTimestamp=${now}&endTimestamp=${now + 100}" +
                        "&scope=test-scope" +
                        "&bookId=test-book"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.INTERNAL_SERVER_ERROR.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"error":"ignore"}
                          |
                        """.trimMargin()
                    )
            }
        }
    }
}