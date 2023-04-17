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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.lwdataprovider.util.CradleResult
import com.exactpro.th2.lwdataprovider.util.GroupBatch
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
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

internal class TestGetMessageGroupsServlet : AbstractHttpHandlerTest<GetMessageGroupsServlet>() {
    override fun createHandler(): GetMessageGroupsServlet {
        return GetMessageGroupsServlet(
            configuration,
            context.convExecutor,
            sseResponseBuilder,
            context.keepAliveHandler,
            context.searchMessagesHandler,
            context.requestsDataMeasurement,
        )
    }

    @Test
    fun `reports error if book is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=test"
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
    fun `reports error if group is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&bookId=test"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("group")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("EMPTY_COLLECTION")
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["", "   "])
    fun `reports error if group is blank`(emptyGroup: String) {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=$emptyGroup" +
                        "&bookId=test"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("group")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("BLANK_GROUP")
            }
        }
    }

    @Test
    fun `response contains only expected alias`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(createCradleStoredMessage("test-${it % 3}", Direction.FIRST, index++, timestamp = start))
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=test-group" +
                        "&bookId=test-book" +
                        "&responseFormat=BASE_64" +
                        "&stream=test-0"
            )

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """id: 1
                          |event: message
                          |data: {"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test:test-0:1:${expectedTimestamp}:1"}
                          |
                          |id: 2
                          |event: message
                          |data: {"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test:test-0:1:${expectedTimestamp}:4"}
                          |
                          |event: close
                          |data: empty data
                          |
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `response contains only expected alias and direction`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(createCradleStoredMessage(
                                "test-${it % 3}",
                                if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                                index++,
                                timestamp = start,
                            ))
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=test-group" +
                        "&bookId=test-book" +
                        "&responseFormat=BASE_64" +
                        "&stream=test-0:1"
            )

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """id: 1
                          |event: message
                          |data: {"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test:test-0:1:${expectedTimestamp}:1"}
                          |
                          |event: close
                          |data: empty data
                          |
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }
}