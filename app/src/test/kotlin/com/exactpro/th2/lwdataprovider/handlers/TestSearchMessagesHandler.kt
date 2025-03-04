/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.util.CradleResult
import com.exactpro.th2.lwdataprovider.util.DummyDataMeasurement
import com.exactpro.th2.lwdataprovider.util.GroupBatch
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.ListCradleResult
import com.exactpro.th2.lwdataprovider.util.createBatches
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import com.exactpro.th2.lwdataprovider.util.validateMessagesOrder
import com.exactpro.th2.lwdataprovider.workers.CradleRequestId
import org.junit.jupiter.api.Assertions.assertEquals
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
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.hasSize
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

    private val searchHandler = createSearchMessagesHandler(decoder, false)

    @Test
    fun `excludes alias from group search`() {
        var index = 1L
        val start = Instant.now()
        val groupBatch = GroupBatch(
            "test-group",
            messages = buildList {
                repeat(6) {
                    add(createCradleStoredMessage("test-${it % 3}", Direction.FIRST, index++))
                }
            },
        )
        doReturn(
            CradleResult(
                groupBatch
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group"
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement, 4))
        searchHandler.loadMessageGroups(
            MessagesGroupRequest(
                groups = setOf("test-group"),
                startTimestamp = start,
                endTimestamp = Instant.now(),
                keepOpen = false,
                bookId = BookId("test-book"),
                responseFormats = setOf(ResponseFormat.BASE_64),
                includeStreams = setOf(
                    ProviderMessageStream("test-0", Direction.FIRST),
                ),
            ),
            handler,
            measurement,
        )

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(2)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).elementsEquals(
            groupBatch.messages.filter { it.sessionAlias == "test-0" && it.direction == Direction.FIRST },
            isParsed = false
        )
        verifyNoInteractions(decoder)
    }

    @Test
    fun `excludes alias and direction from group search`() {
        var index = 1L
        val start = Instant.now()
        val groupBatch = GroupBatch(
            "test-group",
            messages = buildList {
                repeat(6) {
                    add(
                        createCradleStoredMessage(
                            "test-${it % 3}",
                            if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                            index++,
                        )
                    )
                }
            },
        )
        doReturn(
            CradleResult(
                groupBatch
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group"
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement, 4))
        searchHandler.loadMessageGroups(
            MessagesGroupRequest(
                groups = setOf("test-group"),
                startTimestamp = start,
                endTimestamp = Instant.now(),
                keepOpen = false,
                bookId = BookId("test-book"),
                responseFormats = setOf(ResponseFormat.BASE_64),
                includeStreams = setOf(ProviderMessageStream("test-0", Direction.FIRST)),
            ),
            handler,
            measurement,
        )

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(1)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).elementsEquals(
            groupBatch.messages.filter { it.sessionAlias == "test-0" && it.direction == Direction.FIRST },
            isParsed = false
        )
        verifyNoInteractions(decoder)
    }

    @Test
    fun `stops when limit per request is reached`() {
        val taskExecutor = Executors.newSingleThreadExecutor()
        val storedMessages: MutableList<StoredMessage> = arrayListOf(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 3),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 4),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 5),
        )
        doReturn(
            ListCradleResult(storedMessages)
        ).whenever(storage).getMessages(argThat {
            sessionAlias == "test-stream" && direction == Direction.FIRST
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement, 4))
        val future = taskExecutor.submit {
            searchHandler.loadMessages(
                createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
                handler,
                measurement,
            )
        }

        verify(decoder, timeout(200).times(1)).sendBatchMessage(any<MessageGroupBatch.Builder>(), any(), any())
        verify(handler, never()).complete()

        expectThat(decoder.protoQueue.size).isEqualTo(3)
        val offset = 3
        repeat(offset) {
            decoder.protoQueue.poll()?.apply {
                protoParsedMessages = listOf(message("Test$it").build())
                responseMessage()
            }
        }
        future.get(100, TimeUnit.MILLISECONDS)

        expectThat(decoder.protoQueue.size).isEqualTo(2)
        decoder.protoQueue.forEachIndexed { index, details ->
            details.protoParsedMessages = listOf(message("Test${index + offset}").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(5)).handleNext(messages.capture())
            verify(handler).complete()
        }

        expectThat(messages.allValues).elementsEquals(storedMessages)
    }

    @Test
    fun `splits messages by batch size`() {
        val storedMessages = arrayListOf(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 3),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 4),
        )
        doReturn(
            ListCradleResult(storedMessages)
        ).whenever(storage).getMessages(argThat {
            sessionAlias == "test-stream" && direction == Direction.FIRST
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessages(
            createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
            handler,
            measurement,
        )

        verify(decoder, times(2)).sendBatchMessage(any<MessageGroupBatch.Builder>(), any(), any())
        verify(handler, never()).complete()

        expectThat(decoder.protoQueue.size).isEqualTo(4)
        decoder.protoQueue.forEachIndexed { index, details ->
            details.protoParsedMessages = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(4)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).elementsEquals(storedMessages)
    }

    @Test
    fun `returns raw messages`() {
        val storedMessages = arrayListOf(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
        )
        doReturn(
            ListCradleResult(storedMessages)
        ).whenever(storage).getMessages(argThat {
            sessionAlias == "test-stream" && direction == Direction.FIRST
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
        expectThat(messages.allValues).elementsEquals(storedMessages, isParsed = false)
        verifyNoInteractions(decoder)
    }

    @Test
    fun `returns parsed messages`() {
        val storedMessages = arrayListOf(
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 1),
            createCradleStoredMessage("test-stream", Direction.FIRST, index = 2),
        )
        doReturn(
            ListCradleResult(storedMessages)
        ).whenever(storage).getMessages(argThat {
            sessionAlias == "test-stream" && direction == Direction.FIRST
        })

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessages(
            createSearchRequest(listOf(ProviderMessageStream("test-stream", Direction.FIRST)), isRawOnly = false),
            handler,
            measurement,
        )

        verify(handler, never()).complete()

        expectThat(decoder.protoQueue.size).isEqualTo(2)
        decoder.protoQueue.forEachIndexed { index, details ->
            details.protoParsedMessages = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler, decoder) {
            verify(handler, times(2)).handleNext(messages.capture())
            verify(decoder).sendBatchMessage(any<MessageGroupBatch.Builder>(), any(), any())
            verify(handler).complete()
        }
        expectThat(messages.allValues).elementsEquals(storedMessages)
    }

    @Test
    fun `returns parsed messages transport mode`() {
        val decoder = spy(TestDecoder())
        val searchHandler = createSearchMessagesHandler(decoder, true)

        val startTimestamp = Instant.now()
        val endTimestamp = startTimestamp.plus(10L, ChronoUnit.MINUTES)
        val messagesCount = 10
        val increase = 5L
        val group = "test"

        val batches = createBatches(10, 1, 0, increase, startTimestamp, endTimestamp)

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group
        })).thenReturn(ImmutableListCradleResult(batches))

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        val request = MessagesGroupRequest(
            groups = setOf("test"),
            startTimestamp,
            endTimestamp,
            keepOpen = false,
            BookId("test"),
        )
        searchHandler.loadMessageGroups(
            request,
            handler,
            measurement,
        )

        verify(handler, never()).complete()

        assertEquals(messagesCount, decoder.transportQueue.size)
        assertEquals(0, decoder.protoQueue.size)
        decoder.transportQueue.forEachIndexed { index, details ->
            details.protoParsedMessages = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val captor = argumentCaptor<RequestedMessageDetails>()
        verify(handler, atMost(messagesCount)).handleNext(captor.capture())
        verify(handler, never()).writeErrorMessage(any<String>(), any(), any())
        verify(handler, never()).writeErrorMessage(any<Throwable>(), any(), any())
        val messages: List<RequestedMessageDetails> = captor.allValues
        assertEquals(messagesCount, messages.size) {
            val missing: List<StoredMessage> =
                (batches.asSequence() + batches.asSequence()).flatMap { it.messages }.filter { stored ->
                    messages.none {
                        val raw = it.storedMessage
                        raw.sessionAlias == stored.sessionAlias && raw.sequence == stored.sequence && raw.direction == stored.direction
                    }
                }.toList()
            "Missing ${missing.size} message(s): $missing"
        }
        validateMessagesOrder(messages, messagesCount)
    }

    @Test
    fun `returns single parsed message`() {
        val messageId = StoredMessageId(BookId("test"), "test-stream", Direction.FIRST, Instant.now(), 1)
        val message = createCradleStoredMessage("test-stream", Direction.FIRST, index = 1)
        doReturn(
            message
        ).whenever(storage).getMessage(eq(messageId))

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadOneMessage(
            GetMessageRequest(
                messageId,
                onlyRaw = false
            ),
            handler,
            measurement,
        )

        verify(handler, never()).complete()

        expectThat(decoder.protoQueue.size).isEqualTo(1)
        decoder.protoQueue.forEachIndexed { index, details ->
            details.protoParsedMessages = listOf(message("Test$index").build())
            details.responseMessage()
        }

        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).single().get { awaitAndGet() }.equalsMessage(message)
    }

    @Test
    fun `returns single raw message`() {
        val messageId = StoredMessageId(BookId("test"), "test-stream", Direction.FIRST, Instant.now(), 1)
        val message = createCradleStoredMessage("test-stream", Direction.FIRST, index = 1)
        doReturn(
            message,
        ).whenever(storage).getMessage(eq(messageId))

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadOneMessage(
            GetMessageRequest(messageId, onlyRaw = true),
            handler,
            measurement,
        )
        val messages = argumentCaptor<RequestedMessageDetails>()
        inOrder(handler) {
            verify(handler, times(1)).handleNext(messages.capture())
            verify(handler).complete()
        }
        expectThat(messages.allValues).single().get { awaitAndGet() }.equalsMessage(message, isParsed = false)
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

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == startTimestamp && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(firstBatches))
        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == firstBatches.maxOf { it.lastTimestamp } && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(lastBatches))
        whenever(storage.getGroupedMessageBatches(argThat {
            limit == 1 && groupName == group
        })).thenReturn(ImmutableListCradleResult(outsideBatches))

        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        val request = MessagesGroupRequest(
            groups = setOf("test"),
            startTimestamp,
            endTimestamp,
            keepOpen = true,
            BookId("test"),
            responseFormats = setOf(ResponseFormat.BASE_64),
        )
        searchHandler.loadMessageGroups(request, handler, measurement)

        val captor = argumentCaptor<RequestedMessageDetails>()
        verify(handler, atMost(messagesCount)).handleNext(captor.capture())
        verify(handler, never()).writeErrorMessage(any<String>(), any(), any())
        verify(handler, never()).writeErrorMessage(any<Throwable>(), any(), any())
        val messages: List<RequestedMessageDetails> = captor.allValues
        assertEquals(messagesCount, messages.size) {
            val missing: List<StoredMessage> =
                (firstBatches.asSequence() + lastBatches.asSequence()).flatMap { it.messages }.filter { stored ->
                    messages.none {
                        val raw = it.storedMessage
                        raw.sessionAlias == stored.sessionAlias && raw.sequence == stored.sequence && raw.direction == stored.direction
                    }
                }.toList()
            "Missing ${missing.size} message(s): $missing"
        }
        validateMessagesOrder(messages, messagesCount)
    }

    private fun createSearchMessagesHandler(
        decoder: Decoder,
        useTransportMode: Boolean
    ) = SearchMessagesHandler(
        CradleMessageExtractor(manager, DummyDataMeasurement, false),
        decoder,
        executor,
        Configuration(
            CustomConfigurationClass(
                bufferPerQuery = 4,
                useTransportMode = useTransportMode,
                batchSizeBytes = 300,
            )
        )
    )

    @Test
    fun `different groups does not appear in the same batch`() {
        val timestamp = Instant.now()
        val endTimestamp = timestamp.plus(1, ChronoUnit.DAYS)
        val firstGroupBatch = createBatches(
            messagesPerBatch = 1,
            batchesCount = 1,
            overlapCount = 0,
            increase = 10,
            startTimestamp = timestamp,
            end = endTimestamp,
            group = "first",
        )
        val secondGroupBatch = createBatches(
            messagesPerBatch = 1,
            batchesCount = 1,
            overlapCount = 0,
            increase = 10,
            startTimestamp = timestamp,
            end = endTimestamp,
            group = "second",
        )

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == "first"
        })) doReturn ImmutableListCradleResult(firstGroupBatch)

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == "second"
        })) doReturn ImmutableListCradleResult(secondGroupBatch)

        val request = MessagesGroupRequest(
            groups = setOf("first", "second"),
            startTimestamp = timestamp,
            endTimestamp = endTimestamp,
            responseFormats = setOf(ResponseFormat.JSON_PARSED, ResponseFormat.BASE_64),
            keepOpen = false,
            bookId = BookId("test"),
        )
        val handler = spy(MessageResponseHandlerTestImpl(measurement))
        searchHandler.loadMessageGroups(request, handler, measurement)

        inOrder(decoder) {
            verify(decoder, times(1)).sendBatchMessage(any<MessageGroupBatch.Builder>(), any(), eq("first"))
            verify(decoder, times(1)).sendBatchMessage(any<MessageGroupBatch.Builder>(), any(), eq("second"))
        }
        expectThat(decoder.protoQueue)
            .hasSize(2)
            .withElementAt(0) {
                get { sessionGroup } isEqualTo "first"
            }.withElementAt(1) {
                get { sessionGroup } isEqualTo "second"
            }
    }

    private fun Assertion.Builder<List<RequestedMessageDetails>>.elementsEquals(
        expected: List<StoredMessage>,
        isParsed: Boolean = true
    ) {
        expected.forEachIndexed { index, storedMessage ->
            withElementAt(index) {
                get { awaitAndGet() }.equalsMessage(storedMessage, isParsed, index)
            }
        }
    }

    private fun Assertion.Builder<RequestedMessage>.equalsMessage(
        storedMessage: StoredMessage,
        isParsed: Boolean = true,
        index: Int = 0,
    ) {
        get { requestId } isEqualTo CradleRequestId(storedMessage.id)
        get { protoMessage }.apply {
            if (isParsed) {
                isNotNull().single()
                    .get { messageType } isEqualTo "Test$index"
            } else {
                isNull()
            }
        }
    }

    private fun createSearchRequest(streams: List<ProviderMessageStream>, isRawOnly: Boolean): SseMessageSearchRequest =
        SseMessageSearchRequest(
            startTimestamp = Instant.now(),
            endTimestamp = Instant.now(),
            stream = streams,
            searchDirection = SearchDirection.next,
            resultCountLimit = null,
            keepOpen = false,
            responseFormats = if (isRawOnly) setOf(ResponseFormat.BASE_64) else null,
            resumeFromIdsList = null,
            bookId = BookId("test"),
        )
}

private open class TestDecoder(
    capacity: Int = 10
) : Decoder {
    val protoQueue: Queue<RequestedMessageDetails> = ArrayBlockingQueue(capacity)
    val transportQueue: Queue<RequestedMessageDetails> = ArrayBlockingQueue(capacity)
    override fun sendBatchMessage(
        batchBuilder: MessageGroupBatch.Builder,
        requests: Collection<RequestedMessageDetails>,
        session: String
    ) {
        protoQueue.addAll(requests)
    }

    override fun sendBatchMessage(
        batchBuilder: GroupBatch.Builder,
        requests: Collection<RequestedMessageDetails>,
        session: String
    ) {
        transportQueue.addAll(requests)
    }

    //FIXME: implement for transport
}

private open class MessageResponseHandlerTestImpl(
    measurement: DataMeasurement,
    maxQueue: Int = 0,
) : MessageResponseHandler(measurement, maxQueue) {
    override fun handleNextInternal(data: RequestedMessageDetails) {
    }

    override fun complete() {
    }

    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
    }

    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
    }
}