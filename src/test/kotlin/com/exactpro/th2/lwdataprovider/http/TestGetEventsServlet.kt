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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createEventStoredEvent
import com.exactpro.th2.lwdataprovider.util.toStoredEvent
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.first
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.jackson.hasSize
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.path
import strikt.jackson.textValue
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class TestGetEventsServlet : AbstractHttpHandlerTest<GetEventsServlet>() {
    override fun createHandler(): GetEventsServlet {
        return GetEventsServlet(
            configuration,
            sseResponseBuilder,
            keepAliveHandler,
            eventsHandler,
        )
    }

    @Test
    fun `returns events`() {
        val start = Instant.parse("2020-10-31T01:00:00Z")
        val middleA = Instant.parse("2020-10-31T23:59:59.999999999Z")
        val middleB = Instant.parse("2020-11-01T00:00:00Z")
        val end = start.plus(1, ChronoUnit.DAYS)
        val first = createEventStoredEvent(
            eventId = "a",
            start = start,
            end = middleA,
        )
        val second = createEventStoredEvent(
            eventId = "b",
            start = middleB,
            end = end,
        )
        doReturn(
            ImmutableListCradleResult(listOf(first.toStoredEvent()))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == start && startTimestampTo.value == middleA
        })
        doReturn(
            ImmutableListCradleResult(listOf(second.toStoredEvent()))
        ).whenever(storage).getTestEvents(argThat {
            startTimestampFrom.value == middleB && startTimestampTo.value == end
        })
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/events?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test&scope=test-scope"
            )
            expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                .isNotNull()
                .isEqualTo("""
                    id: 1
                    event: event
                    data: {"eventId":"test:test-scope:20201031010001000000000:a","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":1604188798,"nano":999999999},"startTimestamp":{"epochSecond":1604106001,"nano":0},"parentEventId":null,"successful":true,"bookId":"test","scope":"test-scope","attachedMessageIds":[],"body":[]}

                    id: 2
                    event: event
                    data: {"eventId":"test:test-scope:20201101000001000000000:b","batchId":null,"isBatched":false,"eventName":"test_event","eventType":"test","endTimestamp":{"epochSecond":1604192399,"nano":0},"startTimestamp":{"epochSecond":1604188801,"nano":0},"parentEventId":null,"successful":true,"bookId":"test","scope":"test-scope","attachedMessageIds":[],"body":[]}
                    
                    event: close
                    data: empty data


                """.trimIndent())
        }
    }

    @Test
    fun `reports error if book is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/events?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("bookId")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("NULLCHECK_FAILED")
            }
        }
    }

    @Test
    fun `reports error if scope is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/events?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&bookId=book123"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("scope")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("NULLCHECK_FAILED")
            }
        }
    }
}