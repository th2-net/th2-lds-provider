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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.ListCradleResult
import com.exactpro.th2.lwdataprovider.util.createBatches
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import com.exactpro.th2.lwdataprovider.util.validateOrder
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.spy
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.atMost
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.hasSize
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class TestCradleMessageExtractor {
    private val startTimestamp = Instant.now()
    private val endTimestamp = startTimestamp.plus(10, ChronoUnit.MINUTES)
    private val groupRequestBuffer = 200
    private val measurement: DataMeasurement = mock {
        on { start(any()) } doReturn mock { }
    }

    private lateinit var storage: CradleStorage

    private val messageRouter: MessageRouter<MessageGroupBatch> = mock { }

    private lateinit var manager: CradleManager

    private lateinit var extractor: CradleMessageExtractor

    @BeforeEach
    internal fun setUp() {
        storage = mock { }
        manager = mock { on { this.storage }.thenReturn(storage) }
        extractor = CradleMessageExtractor(groupRequestBuffer, manager)
        clearInvocations(storage, messageRouter, manager)
    }

    @Test
    fun `get partially overlapped messages with sink interval`() {
        fun generateMessage(alias: String, start: Instant): List<StoredMessage> {
            var time = start
            return arrayListOf<StoredMessage>().apply {
                repeat(5) {
                    add(createCradleStoredMessage(alias, Direction.FIRST, (it + 1).toLong(), timestamp = time))
                    time = time.plus(10, ChronoUnit.MINUTES)
                }
            }
        }
        var start = Instant.now()
        // -- 10 minutes
        // a 1-2-|3-4|-5
        // b --1-|2-3|-4-5|
        // c ----|--1|-2-3|-4-5
        val messagesByAlias: Map<String, List<StoredMessage>> = listOf("a", "b", "c").associateWith {
            generateMessage(it, start).also {
                start = start.plus(15, ChronoUnit.MINUTES)
            }
        }
        messagesByAlias.forEach { (alias, messages) ->
            doReturn(ImmutableListCradleResult(messages)).whenever(storage).getMessages(argThat {
                sessionAlias == alias
            })
        }
        val sink = StoredMessageDataSink()
        extractor.getMessagesWithSyncInterval(
            messagesByAlias.keys.map { createMessageFilter(it) },
            Duration.ofMinutes(20),
            sink,
            measurement,
        )
        expectThat(sink.messages)
            .hasSize(messagesByAlias.values.sumOf { it.size })
            .containsExactly(
                messagesByAlias.getValue("a")[0],
                messagesByAlias.getValue("a")[1],
                messagesByAlias.getValue("b")[0],
                messagesByAlias.getValue("a")[2],
                messagesByAlias.getValue("a")[3],
                messagesByAlias.getValue("b")[1],
                messagesByAlias.getValue("b")[2],
                messagesByAlias.getValue("c")[0],
                messagesByAlias.getValue("a")[4], // last A
                messagesByAlias.getValue("b")[3],
                messagesByAlias.getValue("b")[4], // last B
                messagesByAlias.getValue("c")[1],
                messagesByAlias.getValue("c")[2],
                messagesByAlias.getValue("c")[3],
                messagesByAlias.getValue("c")[4], // last C
            )
    }

    @Test
    fun `get not overlapped messages with sink interval`() {
        fun generateMessage(alias: String, start: Instant): List<StoredMessage> {
            var time = start
            return arrayListOf<StoredMessage>().apply {
                repeat(5) {
                    add(createCradleStoredMessage(alias, Direction.FIRST, (it + 1).toLong(), timestamp = time))
                    time = time.plus(10, ChronoUnit.MINUTES)
                }
            }
        }
        var start = Instant.now()
        // -- 10 minutes
        // a 1-2-3-4-5
        // b ----------1-2-3-4-5
        // c --------------------1-2-3-4-5
        val messagesByAlias: Map<String, List<StoredMessage>> = listOf("a", "b", "c").associateWith {
            generateMessage(it, start).also {
                start = start.plus(50, ChronoUnit.MINUTES)
            }
        }
        messagesByAlias.forEach { (alias, messages) ->
            doReturn(ImmutableListCradleResult(messages)).whenever(storage).getMessages(argThat {
                sessionAlias == alias
            })
        }
        val sink = StoredMessageDataSink()
        extractor.getMessagesWithSyncInterval(
            messagesByAlias.keys.map { createMessageFilter(it) },
            Duration.ofMinutes(20),
            sink,
            measurement,
        )
        expectThat(sink.messages)
            .hasSize(messagesByAlias.values.sumOf { it.size })
            .containsExactly(
                messagesByAlias.getValue("a") + messagesByAlias.getValue("b") + messagesByAlias.getValue("c"),
            )
    }

    @Test
    fun `get fully overlapped messages with sink interval`() {
        fun generateMessage(alias: String, start: Instant): List<StoredMessage> {
            var time = start
            return arrayListOf<StoredMessage>().apply {
                repeat(5) {
                    add(createCradleStoredMessage(alias, Direction.FIRST, (it + 1).toLong(), timestamp = time))
                    time = time.plus(10, ChronoUnit.MINUTES)
                }
            }
        }
        val start = Instant.now()
        // -- 10 minutes
        // a 1-2-|3-4-|5
        // b 1-2-|3-4-|5
        // c 1-2-|3-4-|5
        val messagesByAlias: Map<String, List<StoredMessage>> = listOf("a", "b", "c").associateWith {
            generateMessage(it, start)
        }
        messagesByAlias.forEach { (alias, messages) ->
            doReturn(ImmutableListCradleResult(messages)).whenever(storage).getMessages(argThat {
                sessionAlias == alias
            })
        }
        val sink = StoredMessageDataSink()
        extractor.getMessagesWithSyncInterval(
            messagesByAlias.keys.map { createMessageFilter(it) },
            Duration.ofMinutes(20),
            sink,
            measurement,
        )
        val messageInInterval = 2
        expectThat(sink.messages)
            .hasSize(messagesByAlias.values.sumOf { it.size })
            .containsExactly(
                messagesByAlias.getValue("a").take(messageInInterval) +
                        messagesByAlias.getValue("b").take(messageInInterval) +
                        messagesByAlias.getValue("c").take(messageInInterval) +
                messagesByAlias.getValue("a").drop(messageInInterval).take(messageInInterval) +
                        messagesByAlias.getValue("b").drop(messageInInterval).take(messageInInterval) +
                        messagesByAlias.getValue("c").drop(messageInInterval).take(messageInInterval) +
                messagesByAlias.getValue("a").takeLast(1) +
                        messagesByAlias.getValue("b").takeLast(1) +
                        messagesByAlias.getValue("c").takeLast(1)
            )
    }

    private fun createMessageFilter(alias: String): MessageFilter = MessageFilterBuilder()
        .bookId(BookId("test"))
        .sessionAlias(alias)
        .direction(Direction.FIRST)
        .build()

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
        val batchesList: MutableList<StoredGroupedMessageBatch> = createBatches(
            messagesPerBatch = messagesPerBatch,
            batchesCount = batchesCount,
            overlapCount = overlap,
            increase = increase,
            startTimestamp = startTimestamp,
            end = endTimestamp,
        ).toMutableList()
        whenever(storage.getGroupedMessageBatches(any())).thenReturn(ListCradleResult(batchesList))

        val sink = spy(StoredMessageDataSink())
        extractor.getMessagesGroup(
            GroupedMessageFilter.builder()
                .groupName("test")
                .timestampFrom().isGreaterThanOrEqualTo(startTimestamp)
                .timestampTo().isLessThan(endTimestamp)
                .build(), CradleGroupRequest(true), sink, measurement
        )

        verify(sink, atMost(messagesCount.toInt())).onNext(any(), any<Collection<StoredMessage>>())
        verify(sink, never()).onError(any<String>(), any(), any())
        val messages = sink.messages
        Assertions.assertEquals(messagesCount.toInt(), messages.size) {
            "Unexpected messages count: $messages"
        }
        validateOrder(messages, messagesCount.toInt())
    }
}

private open class StoredMessageDataSink : MessageDataSink<String, StoredMessage> {
    val messages: MutableList<StoredMessage> = arrayListOf()
    override val canceled: CancellationReason?
        get() = null

    override fun onError(message: String, id: String?, batchId: String?) {
    }

    override fun completed() {
    }

    override fun onNext(marker: String, data: StoredMessage) {
        messages += data
    }
}