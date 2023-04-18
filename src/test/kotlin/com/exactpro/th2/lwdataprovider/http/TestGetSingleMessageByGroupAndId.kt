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

class TestGetSingleMessageByGroupAndId : AbstractHttpHandlerTest<GetSingleMessageByGroupAndId>() {
    override fun createHandler(): GetSingleMessageByGroupAndId {
        return GetSingleMessageByGroupAndId(
            searchHandler = context.searchMessagesHandler,
            configuration = configuration,
            sseResponseBuilder = sseResponseBuilder,
            convExecutor = context.convExecutor,
            dataMeasurement = context.requestsDataMeasurement,
        )
    }

    @Test
    fun `reports error if message ID is incorrect`() {
        startTest { _, client ->
            val response = client.sse(
                "/message/group/test/messageID"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                get { body }.isNotNull()
                    .get { bytes().toString(Charsets.UTF_8) }
                    .isEqualTo("CradleIdException: Message ID (messageID) should contain book ID, session alias, direction, timestamp and sequence number delimited with ':'")
            }
        }
    }

    @Test
    fun `response contains expected message in raw format`() {
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
                                    "test-${it % 3}", Direction.FIRST, index++, timestamp = start,
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

        val msgTimestamp = StoredMessageIdUtils.timestampToString(start)
        startTest { _, client ->
            val response = client.get(
                "/message/group/test-group/test-book:test-0:1:$msgTimestamp:4?responseFormat=BASE_64"
            )

            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${msgTimestamp}:4"}"""
                    )
            }
        }
    }

    @Test
    fun `response contains expected message in parsed format`() {
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
                                    "test-${it % 3}", Direction.FIRST, index++, timestamp = start,
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

        val msgTimestamp = StoredMessageIdUtils.timestampToString(start)
        startTest { _, client ->
            val response = CompletableFuture.supplyAsync {
                client.get(
                    "/message/group/test-group/test-book:test-0:1:$msgTimestamp:4?responseFormat=JSON_PARSED"
                )
            }
            receiveMessages(
                message()
                    .setMetadata(
                        bookName = "test-book",
                        messageType = "Test",
                        direction = com.exactpro.th2.common.grpc.Direction.FIRST,
                        sessionAlias = "test-0",
                        sequence = 4,
                        timestamp = start,
                    ).addField("a", 42)
                    .apply {
                        metadataBuilder.idBuilder.connectionIdBuilder.sessionGroup = "test-group"
                    }
                    .build()
            )

            val seconds = start.epochSecond
            val nanos = start.nano
            val expectedMessage = "{" +
                    "\"timestamp\":{\"epochSecond\":${seconds},\"nano\":${nanos}}," +
                    "\"direction\":\"IN\"," +
                    "\"sessionId\":\"test-0\"," +
                    "\"messageType\":\"Test\"," +
                    "\"attachedEventIds\":[]," +
                    "\"body\":{" +
                        "\"metadata\":{" +
                            "\"id\":{\"connectionId\":{\"sessionAlias\":\"test-0\"},\"direction\":\"FIRST\",\"sequence\":\"4\"," +
                                "\"timestamp\":{\"seconds\":\"$seconds\",\"nanos\":\"$nanos\"},\"subsequence\":[]}," +
                            "\"messageType\":\"Test\"" +
                        "}," +
                        "\"fields\":{\"a\":\"42\"}" +
                    "}," +
                    "\"bodyBase64\":null," +
                    "\"messageId\":\"test-book:test-0:1:${msgTimestamp}:4\"" +
                    "}"
            expectThat(response.get(1, TimeUnit.SECONDS)) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(expectedMessage)
            }
        }
    }
}