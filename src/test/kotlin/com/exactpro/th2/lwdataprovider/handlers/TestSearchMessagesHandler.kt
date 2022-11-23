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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.grpc.toCradleDirection
import com.exactpro.th2.lwdataprovider.util.createBatches
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import com.exactpro.th2.lwdataprovider.util.validateMessagesOrder
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.atMost
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.get
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.single
import strikt.assertions.withElementAt
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Queue
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class TestSearchMessagesHandler {
    private val executor: Executor = Executor { it.run() }
    private val storage = mock<CradleStorage>()
    private val manager = mock<CradleManager> {
        on { storage } doReturn storage
    }

    private val measurement: DataMeasurement = mock {
        on { start(any()) } doReturn mock { }
    }

    private val decoder = spy(TestDecoder())

    private val searchHandler = SearchMessagesHandler(
        CradleMessageExtractor(10, manager),
        decoder,
        executor,
        Configuration(
            CustomConfigurationClass(
                batchSize = 3,
            )
        )
    )

    @Test
    fun `stops when limit per request is reached`() {
        val taskExecutor = Executors.newSingleThreadExecutor()
        doReturn(
            listOf(
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 3),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 4),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 5),
            )
        ).whenever(storage).getMessages(argThat {
            streamName.check("test-stream") && direction.check(Direction.FIRST)
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement, 4))
        val future = taskExecutor.submit {
            searchHandler.loadMessages(
                createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
                handler,
                measurement,
            )
        }

        verify(decoder, timeout(200).times(1)).sendBatchMessage(any(), any(), any())
        verify(handler, never()).handleNext(any())
        verify(handler, never()).complete()

        expectThat(decoder.queue.size).isEqualTo(3)
        val offset = 3
        repeat(offset) {
            decoder.queue.poll()?.apply {
                parsedMessage = listOf(message("Test$it").build())
                responseMessage()
            }
        }
        future.get(100, TimeUnit.MILLISECONDS)

        expectThat(decoder.queue.size).isEqualTo(2)
        decoder.queue.forEachIndexed { index, details ->
            details.parsedMessage = listOf(message("Test${index + offset}").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(5)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues) {
            withElementAt(0) {
                get { id } isEqualTo "test-stream:first:1"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test0"
            }
            withElementAt(1) {
                get { id } isEqualTo "test-stream:first:2"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test1"
            }
            withElementAt(2) {
                get { id } isEqualTo "test-stream:first:3"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test2"
            }
            withElementAt(3) {
                get { id } isEqualTo "test-stream:first:4"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test3"
            }
            withElementAt(4) {
                get { id } isEqualTo "test-stream:first:5"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test4"
            }
        }
    }

    @Test
    fun `splits messages by batch size`() {
        doReturn(
            listOf(
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 3),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 4),
            )
        ).whenever(storage).getMessages(argThat {
            streamName.check("test-stream") && direction.check(Direction.FIRST)
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessages(
            createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
            handler,
            measurement,
        )

        verify(decoder, times(2)).sendBatchMessage(any(), any(), any())
        verify(handler, never()).handleNext(any())
        verify(handler, never()).complete()

        expectThat(decoder.queue.size).isEqualTo(4)
        decoder.queue.forEachIndexed { index, details ->
            details.parsedMessage = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(4)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues) {
            withElementAt(0) {
                get { id } isEqualTo "test-stream:first:1"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test0"
            }
            withElementAt(1) {
                get { id } isEqualTo "test-stream:first:2"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test1"
            }
            withElementAt(2) {
                get { id } isEqualTo "test-stream:first:3"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test2"
            }
            withElementAt(3) {
                get { id } isEqualTo "test-stream:first:4"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test3"
            }
        }
    }

    @Test
    fun `returns raw messages`() {
        doReturn(
            listOf(
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
            )
        ).whenever(storage).getMessages(argThat {
            streamName.check("test-stream") && direction.check(Direction.FIRST)
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessages(
            createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), true),
            handler,
            measurement,
        )
        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(2)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues) {
            get(0).apply {
                get { id } isEqualTo "test-stream:first:1"
                get { parsedMessage }.isNull()
            }
            get(1).apply {
                get { id } isEqualTo "test-stream:first:2"
                get { parsedMessage }.isNull()
            }
        }
        verifyNoInteractions(decoder)
    }

    @Test
    fun `returns parsed messages`() {
        doReturn(
            listOf(
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
                createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
            )
        ).whenever(storage).getMessages(argThat {
            streamName.check("test-stream") && direction.check(Direction.FIRST)
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessages(
            createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
            handler,
            measurement,
        )

        verify(handler, never()).handleNext(any())
        verify(handler, never()).complete()

        expectThat(decoder.queue.size).isEqualTo(2)
        decoder.queue.forEachIndexed { index, details ->
            details.parsedMessage = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler, decoder) {
            verify(decoder).sendBatchMessage(any(), any(), any())
            verify(handler, times(2)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues) {
            withElementAt(0) {
                get { id } isEqualTo "test-stream:first:1"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test0"
            }
            withElementAt(1) {
                get { id } isEqualTo "test-stream:first:2"
                get { parsedMessage }.isNotNull()
                    .single()
                    .get { messageType } isEqualTo "Test1"
            }
        }
    }

    @Test
    fun `returns single parsed message`() {
        doReturn(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1)
        ).whenever(storage).getMessage(
            eq(
                StoredMessageId("test-stream", Direction.FIRST, 1)
            )
        )

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadOneMessage(
            GetMessageRequest(
                "test-stream:first:1",
                onlyRaw = false
            ),
            handler,
            measurement,
        )

        verify(handler, never()).handleNext(any())
        verify(handler, never()).complete()

        expectThat(decoder.queue.size).isEqualTo(1)
        decoder.queue.forEachIndexed { index, details ->
            details.parsedMessage = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(1)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).single().apply {
            get { id } isEqualTo "test-stream:first:1"
            get { parsedMessage }.isNotNull()
                .single()
                .get { messageType } isEqualTo "Test0"
        }
    }

    @Test
    fun `returns single raw message`() {
        doReturn(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
        ).whenever(storage).getMessage(eq(StoredMessageId("test-stream", Direction.FIRST, 1)))

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadOneMessage(
            GetMessageRequest("test-stream:first:1", onlyRaw = true),
            handler,
            measurement,
        )
        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(1)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).single().apply {
            get { id } isEqualTo "test-stream:first:1"
            get { parsedMessage }.isNull()
        }
        verifyNoInteractions(decoder)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `stops pulling if data out of range exist`(offsetNewData: Boolean) {
        val startTimestamp = Instant.now()
        val firstEndTimestamp = startTimestamp.plus(10L, ChronoUnit.MINUTES)
        val endTimestamp = firstEndTimestamp.plus(10L, ChronoUnit.MINUTES)
        val aliasesCount = 5
        val increase = 5L
        val firstBatchMessagesCount = (firstEndTimestamp.epochSecond - startTimestamp.epochSecond) / increase
        val firstMessagesPerAlias = firstBatchMessagesCount / aliasesCount

        val lastBatchMessagesCount = (endTimestamp.epochSecond - firstEndTimestamp.epochSecond) / increase
        val lastMessagesPerAlias = lastBatchMessagesCount / aliasesCount

        val firstBatches = createBatches(
            firstMessagesPerAlias,
            aliasesCount,
            overlapCount = 0,
            increase,
            startTimestamp,
            firstEndTimestamp,
        )
        val lastBatches = createBatches(
            lastMessagesPerAlias,
            aliasesCount,
            overlapCount = 0,
            increase,
            firstEndTimestamp,
            endTimestamp,
            aliasIndexOffset = if (offsetNewData) aliasesCount else 0
        )
        val outsideBatches = createBatches(
            10,
            1,
            0,
            increase,
            endTimestamp.plusNanos(1),
            endTimestamp.plus(5, ChronoUnit.MINUTES),
        )
        val group = "test"
        val firstRequestMessagesCount = firstBatches.sumOf { it.messageCount }
        val secondRequestMessagesCount = lastBatches.sumOf { it.messageCount }
        val messagesCount = firstRequestMessagesCount + secondRequestMessagesCount

        whenever(storage.getGroupedMessageBatches(eq(group), eq(startTimestamp), eq(endTimestamp)))
            .thenReturn(firstBatches)
        whenever(storage.getGroupedMessageBatches(eq(group), eq(firstBatches.maxOf { it.lastTimestamp }), eq(endTimestamp)))
            .thenReturn(lastBatches)
        whenever(storage.getLastMessageBatchForGroup(eq(group))).thenReturn(firstBatches.last(), outsideBatches.last())

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        val request = MessagesGroupRequest(
            groups = setOf("test"),
            startTimestamp,
            endTimestamp,
            sort = true,
            rawOnly = true,
            keepOpen = true
        )
        LOGGER.info { "Request: $request" }
        searchHandler.loadMessageGroups(request, handler, measurement)

        val captor = argumentCaptor<RequestedMessageDetails>()
        verify(handler, atMost(messagesCount)).handleNext(captor.capture())
        val messages: List<RequestedMessageDetails> = captor.allValues
        Assertions.assertEquals(messagesCount, messages.size) {
            val missing: List<StoredMessage> = (firstBatches.asSequence() + lastBatches.asSequence()).flatMap { it.messages }.filter { stored ->
                messages.none {
                    val raw = it.rawMessage
                    raw.sessionAlias == stored.streamName && raw.sequence == stored.index && raw.direction.toCradleDirection() == stored.direction
                }
            }.toList()
            "Missing ${missing.size} message(s): $missing"
        }
        validateMessagesOrder(messages, messagesCount)
    }

    private fun createSearchRequest(streams: List<ProviderMessageStream>, isRawOnly: Boolean): SseMessageSearchRequest = SseMessageSearchRequest(
        startTimestamp = Instant.now(),
        endTimestamp = Instant.now(),
        stream = streams,
        searchDirection = TimeRelation.AFTER,
        resultCountLimit = null,
        keepOpen = false,
        responseFormats = if (isRawOnly) setOf(ResponseFormat.BASE_64) else null,
        resumeFromIdsList = null,
    )

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private open class TestDecoder : Decoder {
    val queue: Queue<RequestedMessageDetails> = ArrayBlockingQueue(10)
    override fun sendBatchMessage(batchBuilder: MessageGroupBatch.Builder, requests: Collection<RequestedMessageDetails>, session: String) {
        queue.addAll(requests)
    }

    override fun sendMessage(message: RequestedMessageDetails, session: String) {
        queue.add(message)
    }
}

private open class MessageResponseHandlerTestImpl(
    measurement: DataMeasurement,
    maxQueue: Int = 0,
) : MessageResponseHandler(measurement, maxQueue) {
    override fun handleNextInternal(data: RequestedMessageDetails) {
    }

    override fun complete() {
    }

    override fun writeErrorMessage(text: String) {
    }

    override fun writeErrorMessage(error: Throwable) {
    }
}