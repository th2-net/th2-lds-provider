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
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.grpc.toGrpcDirection
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.google.protobuf.ByteString
import org.junit.jupiter.api.Assertions
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
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.math.ceil

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class TestCradleMessageExtractor {
    private val startTimestamp = Instant.now()
    private val endTimestamp = startTimestamp.plus(10, ChronoUnit.MINUTES)
    private val batchSize = 100
    private val groupRequestBuffer = 200

    private fun createBatches(messagesPerBatch: Long, batchesCount: Int, overlapCount: Long, increase: Long): List<StoredGroupMessageBatch> =
        ArrayList<StoredGroupMessageBatch>().apply {
            val startSeconds = startTimestamp.epochSecond
            repeat(batchesCount) {
                val start = Instant.ofEpochSecond(startSeconds + it * increase * (messagesPerBatch - overlapCount), startTimestamp.nano.toLong())
                add(StoredGroupMessageBatch().apply {
                    createStoredMessages(
                        "test$it",
                        0,
                        start,
                        messagesPerBatch,
                        direction = if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                        incSeconds = increase,
                        endTimestamp,
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

    private fun checkMessagesReturnsInOrder(messagesPerBatch: Long, batchesCount: Int, increase: Long, messagesCount: Long, overlap: Long) {
        val batchesList: List<StoredGroupMessageBatch> = createBatches(
            messagesPerBatch = messagesPerBatch,
            batchesCount = batchesCount,
            overlapCount = overlap,
            increase = increase
        )
        whenever(storage.getGroupedMessageBatches(eq("test"), eq(startTimestamp), eq(endTimestamp))).thenReturn(batchesList)

        val channelMessages = mock<ResponseHandler> {}
        val context: MessageRequestContext = MockRequestContext(channelMessages)
        extractor.getMessagesGroup("test", startTimestamp, endTimestamp, sort = true, rawOnly = false, requestContext = context)

        val captor = argumentCaptor<MessageGroupBatch>()
        verify(messageRouter, times(ceil(messagesCount.toDouble() / batchSize).toInt())).send(captor.capture(), any())
        val messages = captor.allValues.flatMap { it.groupsList.flatMap { group -> group.messagesList.map { anyMessage -> anyMessage.rawMessage } } }
        Assertions.assertEquals(messagesCount.toInt(), messages.size) {
            "Unexpected messages count: $messages"
        }
        validateOrder(messages, messagesCount.toInt())
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

    private class MockRequestContext(channelMessages: ResponseHandler) : MessageRequestContext(channelMessages) {
        override fun createMessageDetails(
            id: String,
            time: Long,
            storedMessage: StoredMessage,
            responseFormats: List<String>,
            onResponse: () -> Unit
        ): RequestedMessageDetails {
            return createMockDetails(id)
        }

        override fun addStreamInfo() {
            TODO("Not yet implemented")
        }

        private fun createMockDetails(id: String): RequestedMessageDetails = mock { on { this.id }.thenReturn(id) }
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
}