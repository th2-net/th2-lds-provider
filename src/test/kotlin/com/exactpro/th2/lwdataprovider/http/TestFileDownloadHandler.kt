/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.Order
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.setMetadata
import com.exactpro.th2.lwdataprovider.util.CradleResult
import com.exactpro.th2.lwdataprovider.util.GroupBatch
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class TestFileDownloadHandler : AbstractHttpHandlerTest<FileDownloadHandler>() {
    override fun createHandler(): FileDownloadHandler {
        return FileDownloadHandler(
            configuration,
            convExecutor = context.convExecutor,
            sseResponseBuilder,
            keepAliveHandler = context.keepAliveHandler,
            searchMessagesHandler = context.searchMessagesHandler,
            context.requestsDataMeasurement,
        )
    }


    @Test
    fun `response with raw messages`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(
                                createCradleStoredMessage(
                                    "test-${it % 3}",
                                    Direction.FIRST,
                                    index++,
                                    timestamp = start,
                                    book = "test-book",
                                )
                            )
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.get(
                "/download/messages?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=test-group" +
                        "&bookId=test-book" +
                        "&responseFormat=BASE_64"
            )

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:2"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:3"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:4"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:5"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:6"}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `response with parsed messages`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(
                                createCradleStoredMessage(
                                    "test-${it % 3}",
                                    Direction.FIRST,
                                    index++,
                                    timestamp = start,
                                    book = "test-book",
                                )
                            )
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = CompletableFuture.supplyAsync {
                client.get(
                    "/download/messages?" +
                            "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                            "&group=test-group" +
                            "&bookId=test-book" +
                            "&responseFormat=JSON_PARSED"
                )
            }

            val parsedResponses = (0 until 6).map {
                message()
                    .setMetadata(
                        bookName = "test-book",
                        messageType = "Test",
                        direction = com.exactpro.th2.common.grpc.Direction.FIRST,
                        sessionAlias = "test-${it % 3}",
                        sequence = (it + 1).toLong(),
                        timestamp = start,
                    ).addField("a", it)
                    .apply {
                        metadataBuilder.idBuilder.connectionIdBuilder.sessionGroup = "test-group"
                    }
                    .build()
            }.toTypedArray()

            receiveMessagesGroup(*parsedResponses)

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response.get(1, TimeUnit.SECONDS)) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-0","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-0"},"direction":"FIRST","sequence":"1","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"0"}},"messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                          |{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-1","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-1"},"direction":"FIRST","sequence":"2","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"1"}},"messageId":"test-book:test-1:1:${expectedTimestamp}:2"}
                          |{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-2","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-2"},"direction":"FIRST","sequence":"3","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"2"}},"messageId":"test-book:test-2:1:${expectedTimestamp}:3"}
                          |{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-0","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-0"},"direction":"FIRST","sequence":"4","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"3"}},"messageId":"test-book:test-0:1:${expectedTimestamp}:4"}
                          |{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-1","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-1"},"direction":"FIRST","sequence":"5","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"4"}},"messageId":"test-book:test-1:1:${expectedTimestamp}:5"}
                          |{"timestamp":{"epochSecond":$seconds,"nano":$nanos},"direction":"IN","sessionId":"test-2","messageType":"Test","attachedEventIds":[],"body":{"metadata":{"id":{"connectionId":{"sessionAlias":"test-2"},"direction":"FIRST","sequence":"6","timestamp":{"seconds":"$seconds","nanos":"$nanos"},"subsequence":[]},"messageType":"Test"},"fields":{"a":"5"}},"messageId":"test-book:test-2:1:${expectedTimestamp}:6"}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `response with limit raw messages`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(
                                createCradleStoredMessage(
                                    "test-${it % 3}",
                                    Direction.FIRST,
                                    index++,
                                    timestamp = start,
                                    book = "test-book",
                                )
                            )
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val response = client.get(
                "/download/messages?" +
                        "startTimestamp=${start.toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&group=test-group" +
                        "&bookId=test-book" +
                        "&responseFormat=BASE_64" +
                        "&limit=4"
            )

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:2"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:3"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:4"}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `response with messages in reversed order`() {
        var index = 1L
        val start = Instant.now()
        doReturn(
            CradleResult(
                GroupBatch(
                    "test-group",
                    book = "test-book",
                    messages = buildList {
                        repeat(6) {
                            add(
                                createCradleStoredMessage(
                                    "test-${it % 3}",
                                    Direction.FIRST,
                                    index++,
                                    timestamp = start,
                                    book = "test-book",
                                )
                            )
                        }
                    },
                )
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book" && order == Order.REVERSE
        })

        startTest { _, client ->
            val response = client.get(
                "/download/messages?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${start.toEpochMilli()}" +
                        "&group=test-group" +
                        "&bookId=test-book" +
                        "&responseFormat=BASE_64" +
                        "&searchDirection=previous"
            )

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:6"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:5"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:4"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:3"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:2"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }
}