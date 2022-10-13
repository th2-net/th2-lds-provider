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
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.entities.requests.MessageRequestKind.RAW_AND_PARSE
import com.exactpro.th2.lwdataprovider.entities.requests.MessageRequestKind.RAW_WITHOUT_SENDING_TO_CODEC
import com.exactpro.th2.lwdataprovider.entities.requests.MessageRequestKind.RAW_WITH_SENDING_TO_CODEC
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.grpc.toCradleDirection
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.prometheus.client.Counter
import mu.KotlinLogging
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.spy
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import kotlin.math.ceil

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class TestCradleMessageExtractor {
    private val startTimestamp = Instant.now()
    private val endTimestamp = startTimestamp.plus(10, ChronoUnit.MINUTES)
    private val batchSize = 100
    private val groupRequestBuffer = 200

    private fun createBatches(
        messagesPerBatch: Long,
        batchesCount: Int,
        overlapCount: Long,
        increase: Long,
        startTimestamp: Instant = this.startTimestamp,
        end: Instant = endTimestamp,
        aliasIndexOffset: Int = 0,
    ): List<StoredGroupMessageBatch> =
        ArrayList<StoredGroupMessageBatch>().apply {
            val startSeconds = startTimestamp.epochSecond
            repeat(batchesCount) {
                val start = Instant.ofEpochSecond(startSeconds + it * increase * (messagesPerBatch - overlapCount), startTimestamp.nano.toLong())
                add(StoredGroupMessageBatch().apply {
                    createStoredMessages(
                        "test${it + aliasIndexOffset}",
                        Instant.now().run { epochSecond * 1_000_000_000 + nano },
                        start,
                        messagesPerBatch,
                        direction = if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                        incSeconds = increase,
                        end,
                    ).forEach(this::addMessage)
                })
            }
        }

    private lateinit var storage: CradleStorage

    private val configuration = Configuration(CustomConfigurationClass(
        maxBufferDecodeQueue = 1000, // to avoid blocking during extraction
        batchSize = batchSize,
        groupRequestBuffer = groupRequestBuffer,
    ))

    private val messageRouter: MessageRouter<MessageGroupBatch> = mock { }

    private lateinit var manager: CradleManager

    private lateinit var extractor: CradleMessageExtractor

    @BeforeEach
    internal fun setUp() {
        storage = mock { }
        manager = mock { on { this.storage }.thenReturn(storage) }
        extractor = CradleMessageExtractor(configuration, manager, RabbitMqDecoder(
            configuration,
            messageRouter,
            messageRouter,
        ))
        clearInvocations(storage, messageRouter, manager)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `stops pulling if data out of range exist`(offsetNewData: Boolean) {
        // FIXME: should be moved to a separate test but for now leave it here
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
        val firstRequestMessagesCount = firstBatches.sumOf { it.messageCount }
        val secondRequestMessagesCount = lastBatches.sumOf { it.messageCount }
        val messagesCount = firstRequestMessagesCount + secondRequestMessagesCount

        whenever(storage.getGroupedMessageBatches(eq(GROUP_NAME), eq(startTimestamp), eq(endTimestamp)))
            .thenReturn(firstBatches)
        whenever(storage.getGroupedMessageBatches(eq(GROUP_NAME), eq(firstBatches.maxOf { it.lastTimestamp }), eq(endTimestamp)))
            .thenReturn(lastBatches)
        whenever(storage.getLastMessageBatchForGroup(eq(GROUP_NAME))).thenReturn(firstBatches.last(), outsideBatches.last())

        val channelMessages = mock<ResponseHandler<MockEvent>> {}
        val context: MessageRequestContext<MockEvent> = MockRequestContext(channelMessages)
        val handler = SearchMessagesHandler(extractor, Executors.newSingleThreadExecutor())
        val request = MessagesGroupRequest(
            groups = setOf(GROUP_NAME),
            startTimestamp,
            endTimestamp,
            sort = true,
            kind = RAW_AND_PARSE,
            keepOpen = true
        )
        LOGGER.info { "Request: $request" }
        handler.loadMessageGroups(request, context)


        val firstInvocations = ceil(firstRequestMessagesCount.toDouble() / batchSize).toInt()
        val secondInvocations = ceil(secondRequestMessagesCount.toDouble() / batchSize).toInt()
        val captor = argumentCaptor<MessageGroupBatch>()
        verify(messageRouter, timeout(100000).times(firstInvocations + secondInvocations)).send(captor.capture(), any())
        val messages = captor.allValues.flatMap { it.groupsList.flatMap { group -> group.messagesList.map { anyMessage -> anyMessage.rawMessage } } }
        Assertions.assertEquals(messagesCount, messages.size) {
            val missing: List<StoredMessage> = (firstBatches.asSequence() + lastBatches.asSequence()).flatMap { it.messages }.filter { stored ->
                messages.none {
                    it.sessionAlias == stored.streamName && it.sequence == stored.index && it.direction.toCradleDirection() == stored.direction
                }
            }.toList()
            "Missing ${missing.size} message(s): $missing"
        }
        validateOrder(messages, messagesCount)
    }

    @Test
    fun getMessagesGroupWithOverlapping() {
        val batchesCount = 5
        val increase = 5L
        val messagesCount = (endTimestamp.epochSecond - startTimestamp.epochSecond) / increase
        val messagesPerBatch = messagesCount / batchesCount
        checkMessagesReturnsInOrder(messagesPerBatch, batchesCount, increase, messagesCount, overlap = messagesPerBatch / 2)
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 5, 10])
    fun getMessagesGroupWithoutOverlapping(batchesCount: Int) {
        val increase = 1L
        val messagesCount = (endTimestamp.epochSecond - startTimestamp.epochSecond) / increase
        val messagesPerBatch = messagesCount / batchesCount
        checkMessagesReturnsInOrder(messagesPerBatch, batchesCount, increase, messagesCount, overlap = 0)
    }

    @Test
    fun getMessagesGroupWithFullOverlapping() {
        val batchesCount = 5
        val increase = 5L
        val messagesCount = (endTimestamp.epochSecond - startTimestamp.epochSecond) / increase
        val messagesPerBatch = messagesCount / batchesCount
        checkMessagesReturnsInOrder(messagesPerBatch, batchesCount, increase, messagesCount, overlap = messagesPerBatch)
    }

    @Test
    fun `test request kind RAW_AND_PARSE`() {
        val channelMessages = mock<ResponseHandler<MockEvent>> {}
        val context: MessageRequestContext<MockEvent> = spy(MockRequestContext(channelMessages))
        val increase = 1L
        val batchesCount = 5
        val messagesPerBatch = 5L

        configureStorage(messagesPerBatch, batchesCount, 0, increase)
        extractor.getMessagesGroup(GROUP_NAME, CradleGroupRequest(startTimestamp, endTimestamp, sort = false, RAW_AND_PARSE), context)


        val routerCaptor = argumentCaptor<MessageGroupBatch>()
        verify(messageRouter, times(ceil(batchesCount.toDouble() / messagesPerBatch).toInt())).send(routerCaptor.capture(), any())

        val contextCaptor = argumentCaptor<RequestedMessageDetails<MockEvent>>()
        verify(context, times((batchesCount * messagesPerBatch).toInt())).registerMessage(contextCaptor.capture())
        verify(context, times((batchesCount * messagesPerBatch).toInt())).createMessageDetails(any(), any(), any(), any(), any())
    }

    @Test
    fun `test request kind RAW_WITHOUT_SENDING_TO_CODEC`() {
        val channelMessages = mock<ResponseHandler<MockEvent>> {}
        val context: MessageRequestContext<MockEvent> = spy(MockRequestContext(channelMessages))
        val increase = 1L
        val batchesCount = 5
        val messagesPerBatch = 5L

        configureStorage(messagesPerBatch, batchesCount, 0, increase)
        extractor.getMessagesGroup(GROUP_NAME, CradleGroupRequest(startTimestamp, endTimestamp, sort = false, RAW_WITHOUT_SENDING_TO_CODEC), context)


        verify(messageRouter, never()).send(any(), any())

        verify(context.streamInfo, times(batchesCount)).registerMessage(any(), any(), eq(GROUP_NAME))
        verify(context, times((batchesCount * messagesPerBatch).toInt())).createMessageDetails(any(), any(), any(), any(), any())
    }

    @Test
    fun `test request kind RAW_WITH_SENDING_TO_CODEC`() {
        val channelMessages = mock<ResponseHandler<MockEvent>> {}
        val context = spy(MockRequestContext(channelMessages))
        val increase = 1L
        val batchesCount = 5
        val messagesPerBatch = 5L

        configureStorage(messagesPerBatch, batchesCount, 0, increase)
        extractor.getMessagesGroup(GROUP_NAME, CradleGroupRequest(startTimestamp, endTimestamp, sort = false, RAW_WITH_SENDING_TO_CODEC), context)

        val routerCaptor = argumentCaptor<MessageGroupBatch>()
        verify(messageRouter, times(ceil(batchesCount.toDouble() / messagesPerBatch).toInt())).send(routerCaptor.capture(), any())
        assertEquals((batchesCount * messagesPerBatch).toInt(), context.messageDetails.size)
        context.messageDetails.forEach {
            verify(it, times(1)).notifyMessage()
            verify(it, times(1)).responseMessage()
        }

        verify(context.streamInfo, times(batchesCount)).registerMessage(any(), any(), eq(GROUP_NAME))
        verify(context, times((batchesCount * messagesPerBatch).toInt())).createMessageDetails(any(), any(), any(), any(), any())
    }

    private fun checkMessagesReturnsInOrder(messagesPerBatch: Long, batchesCount: Int, increase: Long, messagesCount: Long, overlap: Long) {
        configureStorage(messagesPerBatch, batchesCount, overlap, increase)

        val channelMessages = mock<ResponseHandler<MockEvent>> {}
        val context: MessageRequestContext<MockEvent> = MockRequestContext(channelMessages)
        extractor.getMessagesGroup(GROUP_NAME, CradleGroupRequest(startTimestamp, endTimestamp, sort = true, RAW_AND_PARSE), requestContext = context)

        val captor = argumentCaptor<MessageGroupBatch>()
        verify(messageRouter, times(ceil(messagesCount.toDouble() / batchSize).toInt())).send(captor.capture(), any())
        val messages = captor.allValues.flatMap { it.groupsList.flatMap { group -> group.messagesList.map { anyMessage -> anyMessage.rawMessage } } }
        Assertions.assertEquals(messagesCount.toInt(), messages.size) {
            "Unexpected messages count: $messages"
        }
        validateOrder(messages, messagesCount.toInt())
    }

    private fun configureStorage(
        messagesPerBatch: Long,
        batchesCount: Int,
        overlap: Long,
        increase: Long
    ) {
        val batchesList: List<StoredGroupMessageBatch> = createBatches(
            messagesPerBatch = messagesPerBatch,
            batchesCount = batchesCount,
            overlapCount = overlap,
            increase = increase
        )
        whenever(storage.getGroupedMessageBatches(eq(GROUP_NAME), eq(startTimestamp), eq(endTimestamp))).thenReturn(
            batchesList
        )
    }

    private fun validateOrder(messages: List<RawMessage>, expectedUniqueMessages: Int) {
        var prevMessage: RawMessage? = null
        val ids = HashSet<MessageID>(expectedUniqueMessages)
        for (message in messages) {
            ids += message.metadata.id
            prevMessage?.also {
                Assertions.assertTrue(it.metadata.timestamp.toInstant() <= message.metadata.timestamp.toInstant()) {
                    "Unordered messages: $it and $message"
                }
            }
            prevMessage = message
        }
        Assertions.assertEquals(expectedUniqueMessages, ids.size) {
            "Unexpected number of IDs: $ids"
        }
    }

    private data class MockEvent(val data: String)private class MockRequestContext(
        channelMessages: ResponseHandler
    <MockEvent>) : MessageRequestContext<MockEvent>(
        channelMessages,
        streamInfo = spy(ProviderStreamInfo())
    ) {override val sendResponseCounter: Counter.Child = mock {  }

        val messageDetails = mutableListOf<RequestedMessageDetails<MockEvent>>()

        override fun createMessageDetails(
            id: String,
            time: Long,
            storedMessage: StoredMessage,
            responseFormats: List<String>,
            onResponse: () -> Unit
        ): RequestedMessageDetails<MockEvent> {
            return createMockDetails(id).also(messageDetails::add)
        }

        override fun addStreamInfo() {
            TODO("Not yet implemented")
        }

        private fun createMockDetails(id: String): RequestedMessageDetails<MockEvent> = mock { on { this.id }.thenReturn(id) }
    }

    private fun createStoredMessages(
        alias: String,
        startSequence: Long,
        startTimestamp: Instant,
        count: Long,
        direction: Direction = Direction.FIRST,
        incSeconds: Long = 10L,
        maxTimestamp: Instant,
    ): List<MessageToStore> {
        return (0 until count).map {
            val index = startSequence + it
            val instant = startTimestamp.plusSeconds(incSeconds * it).coerceAtMost(maxTimestamp)
            MessageToStoreBuilder()
                .direction(direction)
                .streamName(alias)
                .index(index)
                .timestamp(instant)
                .content(
                    "abc".toByteArray()
                )
                .metadata("com.exactpro.th2.cradle.grpc.protocol", "abc")
                .build()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val GROUP_NAME = "test"
    }
}