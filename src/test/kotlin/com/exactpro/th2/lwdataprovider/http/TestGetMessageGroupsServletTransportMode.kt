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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.PageId
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53Transport
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import io.javalin.http.HttpStatus
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyVararg
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

internal class TestGetMessageGroupsServletTransportMode : AbstractHttpHandlerTest<GetMessageGroupsServlet>() {
    override val configuration: Configuration
        get() = Configuration(
            CustomConfigurationClass(
                decodingTimeout = 100,
                useTransportMode = true
            )
        )

    override val sseResponseBuilder =
        SseResponseBuilder(context.jacksonMapper, MessageProducer53Transport.Companion::createMessage)

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
    fun `returns parsed message`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val messageBatch = StoredGroupedMessageBatch(
            SESSION_GROUP,
            listOf(
                createCradleStoredMessage(
                    streamName = SESSION_ALIAS,
                    direction = Direction.FIRST,
                    index = 1,
                    content = "test content",
                    timestamp = messageTimestamp,
                )
            ),
            PageId(BookId(BOOK_NAME), PAGE_NAME),
            Instant.now(),
        )

        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage)
            .getGroupedMessageBatches(any())
        doReturn(ImmutableListCradleResult(listOf(messageBatch)))
            .whenever(storage).getGroupedMessageBatches(argThat {
                groupName == SESSION_GROUP && bookId.name == BOOK_NAME
                        && from.value == start && to.value == end
            })

        startTest { _, client ->
            val response = CompletableFuture.supplyAsync {
                client.sse(
                    "/search/sse/messages/group?" +
                            "startTimestamp=${start.toEpochMilli()}" +
                            "&endTimestamp=${end.toEpochMilli()}" +
                            "&bookId=$BOOK_NAME" +
                            "&group=$SESSION_GROUP" +
                            "&responseFormat=BASE_64" +
                            "&responseFormat=JSON_PARSED"
                )
            }
            receiveTransportMessages(
                BOOK_NAME,
                SESSION_GROUP,
                ParsedMessage(
                    id = MessageId(
                        sessionAlias = SESSION_ALIAS,
                        direction = INCOMING,
                        sequence = 1,
                        timestamp = messageTimestamp,
                        subsequence = arrayListOf(1),
                    ),
                    type = MESSAGE_TYPE,
                    rawBody = Unpooled.buffer().apply {
                        writeCharSequence(
                            "{\"unprintable\":\"\\u000135=123\\u0001\",\"int\":\"1\",\"instant\":\"$messageTimestamp\",\"stringList\":[\"a\",\"b\"],\"subMessage\":{\"string\":\"abc\"},\"subMessageList\":[{\"string\":\"def\"},{\"string\":\"ghi\"}]}",
                            Charsets.UTF_8,
                        )
                    }
                ))
            val expectedData =
                "{\"timestamp\":{\"epochSecond\":${messageTimestamp.epochSecond},\"nano\":${messageTimestamp.nano}},\"direction\":\"IN\",\"sessionId\":\"$SESSION_ALIAS\"," +
                        "\"attachedEventIds\":[]," +
                        "\"body\":[{\"metadata\":{\"subsequence\":[1]," +
                        "\"messageType\":\"$MESSAGE_TYPE\"},\"fields\":{\"unprintable\":\"\\u000135=123\\u0001\",\"int\":\"1\",\"instant\":\"$messageTimestamp\",\"stringList\":[\"a\",\"b\"],\"subMessage\":{\"string\":\"abc\"},\"subMessageList\":[{\"string\":\"def\"},{\"string\":\"ghi\"}]}}]," +
                        "\"bodyBase64\":\"dGVzdCBjb250ZW50\",\"messageId\":\"$BOOK_NAME:$SESSION_ALIAS:1:${
                            StoredMessageIdUtils.timestampToString(
                                messageTimestamp
                            )
                        }:1\"}"
            expectThat(response.get(1, TimeUnit.SECONDS)) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """
                      id: 1
                      event: message
                      data: $expectedData
                    
                      event: close
                      data: empty data


                      """.trimIndent()
                    )
            }
        }
    }

    @Test
    fun `returns raw message`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val messageBatch = StoredGroupedMessageBatch(
            SESSION_GROUP,
            listOf(
                createCradleStoredMessage(
                    streamName = SESSION_ALIAS,
                    direction = Direction.FIRST,
                    index = 1,
                    content = "test content",
                    timestamp = messageTimestamp,
                )
            ),
            PageId(BookId(BOOK_NAME), PAGE_NAME),
            Instant.now(),
        )

        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage)
            .getGroupedMessageBatches(any())
        doReturn(ImmutableListCradleResult(listOf(messageBatch)))
            .whenever(storage).getGroupedMessageBatches(argThat {
                groupName == SESSION_GROUP && bookId.name == BOOK_NAME
                        && from.value == start && to.value == end
            })

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=$BOOK_NAME" +
                        "&group=$SESSION_GROUP" +
                        "&responseFormat=BASE_64"
            )
            val expectedData =
                "{\"timestamp\":{\"epochSecond\":${messageTimestamp.epochSecond},\"nano\":${messageTimestamp.nano}},\"direction\":\"IN\",\"sessionId\":\"$SESSION_ALIAS\"," +
                        "\"attachedEventIds\":[]," +
                        "\"bodyBase64\":\"dGVzdCBjb250ZW50\",\"messageId\":\"$BOOK_NAME:$SESSION_ALIAS:1:${
                            StoredMessageIdUtils.timestampToString(
                                messageTimestamp
                            )
                        }:1\"}"
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """
                      id: 1
                      event: message
                      data: $expectedData
                    
                      event: close
                      data: empty data


                      """.trimIndent()
                    )
            }
        }
    }

    @Test
    fun `finishes the request if error happened during sending a batch`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        val messageTimestamp = start.plus(30, ChronoUnit.MINUTES)
        val messageBatch = StoredGroupedMessageBatch(
            SESSION_GROUP,
            listOf(
                createCradleStoredMessage(
                    streamName = SESSION_ALIAS,
                    direction = Direction.FIRST,
                    index = 1,
                    content = "test content",
                    timestamp = messageTimestamp,
                )
            ),
            PageId(BookId(BOOK_NAME), PAGE_NAME),
            Instant.now(),
        )
        doReturn(ImmutableListCradleResult(emptyList<StoredMessage>())).whenever(storage)
            .getGroupedMessageBatches(any())
        doReturn(ImmutableListCradleResult(listOf(messageBatch)))
            .whenever(storage).getGroupedMessageBatches(argThat {
                groupName == SESSION_GROUP && bookId.name == BOOK_NAME
                        && from.value == start && to.value == end
            })
        whenever(transportMessageRouter.send(any(), anyVararg())).doThrow(IllegalStateException("fake"))

        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages/group?" +
                        "startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=$BOOK_NAME" +
                        "&group=$SESSION_GROUP" +
                        "&responseFormat=BASE_64" +
                        "&responseFormat=JSON_PARSED"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """
                      id: 1
                      event: error
                      data: {"id":"$BOOK_NAME:$SESSION_ALIAS:1:${StoredMessageIdUtils.timestampToString(messageTimestamp)}:1","error":"Codec response wasn\u0027t received during timeout"}

                      event: error
                      data: {"error":"fake"}
                    
                      event: close
                      data: empty data


                      """.trimIndent()
                    )
            }
        }
    }
}