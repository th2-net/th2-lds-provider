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
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.setMetadata
import com.exactpro.th2.lwdataprovider.grpc.toRawMessage
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
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

internal class TestGetMessagesServlet : AbstractHttpHandlerTest<GetMessagesServlet>() {
    override fun createHandler(): GetMessagesServlet {
        return GetMessagesServlet(
            configuration,
            sseResponseBuilder,
            context.keepAliveHandler,
            context.searchMessagesHandler,
            context.dataMeasurement,
        )
    }

    @Test
    fun `returns raw message`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val message = createCradleStoredMessage(
            streamName = "test",
            direction = Direction.FIRST,
            index = 1,
            content = "test content",
            timestamp = messageTimestamp,
        )
        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage).getMessages(any())
        doReturn(ImmutableListCradleResult(listOf(message)))
            .whenever(storage).getMessages(argThat {
                sessionAlias == "test" && bookId.name == "test"
                        && timestampFrom.value == start && timestampTo.value == end
                        && direction == Direction.FIRST
            })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages?" +
                        "startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test" +
                        "&stream=test" +
                        "&responseFormat=BASE_64"
            )
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo("""
                      id: 1
                      event: message
                      data: {"timestamp":{"epochSecond":${messageTimestamp.epochSecond},"nano":${messageTimestamp.nano}},"direction":"IN","sessionId":"test","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"dGVzdCBjb250ZW50","messageId":"test:test:1:${StoredMessageIdUtils.timestampToString(messageTimestamp)}:1"}
                    
                      event: close
                      data: empty data


                      """.trimIndent())
            }
        }
    }

    @Test
    fun `returns parsed message`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val message = createCradleStoredMessage(
            streamName = "test",
            direction = Direction.FIRST,
            index = 1,
            content = "test content",
            timestamp = messageTimestamp,
        )
        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage).getMessages(any())
        doReturn(ImmutableListCradleResult(listOf(message)))
            .whenever(storage).getMessages(argThat {
                sessionAlias == "test" && bookId.name == "test"
                        && timestampFrom.value == start && timestampTo.value == end
                        && direction == Direction.FIRST
            })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages?" +
                        "startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test" +
                        "&stream=test" +
                        "&responseFormat=BASE_64" +
                        "&responseFormat=JSON_PARSED"
            )
            receiveMessages(message()
                .setMetadata(
                    bookName = "test",
                    messageType = "Test",
                    direction = com.exactpro.th2.common.grpc.Direction.FIRST,
                    sessionAlias = "test",
                    sequence = 1,
                    timestamp = messageTimestamp,
                ).addField("a", 42)
                .build())
            val expectedData =
                "{\"timestamp\":{\"epochSecond\":${messageTimestamp.epochSecond},\"nano\":${messageTimestamp.nano}},\"direction\":\"IN\",\"sessionId\":\"test\"," +
                        "\"messageType\":\"Test\",\"attachedEventIds\":[]," +
                        "\"body\":{\"metadata\":{\"id\":{\"connectionId\":{\"sessionAlias\":\"test\"},\"direction\":\"FIRST\",\"sequence\":\"1\",\"timestamp\":{\"seconds\":\"${messageTimestamp.epochSecond}\",\"nanos\":\"${messageTimestamp.nano}\"},\"subsequence\":[]}," +
                        "\"messageType\":\"Test\"},\"fields\":{\"a\":\"42\"}}," +
                        "\"bodyBase64\":\"dGVzdCBjb250ZW50\",\"messageId\":\"test:test:1:${StoredMessageIdUtils.timestampToString(messageTimestamp)}:1\"}"
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo("""
                      id: 1
                      event: message
                      data: $expectedData
                    
                      event: close
                      data: empty data


                      """.trimIndent())
            }
        }
    }

    @Test
    fun `returns timeout error message`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val message = createCradleStoredMessage(
            streamName = "test",
            direction = Direction.FIRST,
            index = 1,
            content = "test content",
            timestamp = messageTimestamp,
        )
        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage).getMessages(any())
        doReturn(ImmutableListCradleResult(listOf(message)))
            .whenever(storage).getMessages(argThat {
                sessionAlias == "test" && bookId.name == "test"
                        && timestampFrom.value == start && timestampTo.value == end
                        && direction == Direction.FIRST
            })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages?" +
                        "startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test" +
                        "&stream=test" +
                        "&responseFormat=BASE_64" +
                        "&responseFormat=JSON_PARSED"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo("""
                      id: 1
                      event: error
                      data: {"id":"test:test:1:${StoredMessageIdUtils.timestampToString(messageTimestamp)}:1","error":"Codec response wasn\u0027t received during timeout"}
                    
                      event: close
                      data: empty data


                      """.trimIndent())
            }
        }
    }

    @Test
    fun `reports error if book is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&stream=test"
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
}