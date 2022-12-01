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

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.setMetadata
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.grpc.toGrpcDirection
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.anyVararg
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

internal class TestRabbitMqDecoder {
    private val listeners = arrayListOf<MessageListener<MessageGroupBatch>>()
    private val messageRouter: MessageRouter<MessageGroupBatch> = mock {
        on { subscribeAll(any(), anyVararg()) } doAnswer {
            listeners += it.getArgument<MessageListener<MessageGroupBatch>>(0)
            mock { }
        }
    }

    private val decoder = RabbitMqDecoder(
        maxDecodeQueue = 3,
        messageRouterRawBatch = messageRouter,
        codecUsePinAttributes = true,
    )

    @ParameterizedTest(name = "{0} message(s) in response")
    @ValueSource(ints = [1, 2])
    fun `notifies when response received`(messagesInResponse: Int) {
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = createAndVerifyDetails("test", onResponse = onResponse)

        val parsed = arrayListOf<Message>().apply {
            repeat(messagesInResponse) {
                this += createParsedMessage(details, it)
            }
        }
        notifyListeners(parsed)
        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNotNull()
            .containsExactly(parsed)
    }

    @Test
    fun `notifies when timeout exceeded`() {
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = createAndVerifyDetails("test", onResponse = onResponse)

        decoder.removeOlderThen(0)

        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNull()
    }

    @Test
    fun `notifies on close`() {
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = createAndVerifyDetails("test", onResponse = onResponse)

        decoder.close()

        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNull()
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 2])
    fun `waits until the queue is free`(waitingRequests: Int) {
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details: MutableList<RequestedMessageDetails> = (1..3).map { createAndVerifyDetails("test", it.toLong(), onResponse) }.toMutableList()
        clearInvocations(messageRouter)
        val executor = Executors.newSingleThreadExecutor()
        val futures = arrayListOf<Future<Pair<MessageGroupBatch.Builder, RequestedMessageDetails>>>().apply {
            repeat(waitingRequests) {
                this += executor.submit(Callable { createAndSendDetails("test", (4 + it).toLong(), onResponse) })
            }
        }
        verify(messageRouter, timeout(100).times(0)).send(any(), anyVararg())

        fun messageReceived(det: RequestedMessageDetails) {
            notifyListeners(listOf(createParsedMessage(det)))
        }
        var index = 0
        repeat(waitingRequests) {
            messageReceived(details[index++])
            val (builder, detail) = futures[it].get(100, TimeUnit.MILLISECONDS)
            verifyRouter(builder.build())
            details.add(detail)
        }

        details.asSequence().drop(waitingRequests).forEach { messageReceived(it) }
        expect {
            details.forEach {
                catching { verify(onResponse).invoke(same(it)) }
                that(it).get { parsedMessage }.isNotNull()
            }
        }
    }

    private fun createAndVerifyDetails(
        alias: String,
        index: Long = 1,
        onResponse: (RequestedMessageDetails) -> Unit,
    ): RequestedMessageDetails {
        val (builder, details) = createAndSendDetails(alias, index, onResponse)
        verifyRouter(builder.build())
        return details
    }

    private fun verifyRouter(batch: MessageGroupBatch) {
        verify(messageRouter).send(eq(batch), anyVararg())
        clearInvocations(messageRouter)
    }

    private fun createAndSendDetails(
        alias: String,
        index: Long,
        onResponse: (RequestedMessageDetails) -> Unit,
    ): Pair<MessageGroupBatch.Builder, RequestedMessageDetails> {
        val builder = MessageGroupBatch.newBuilder()
        val details = RequestedMessageDetails(
            createCradleStoredMessage(alias, Direction.FIRST, index),
            group = null,
            onResponse,
        )
        decoder.sendBatchMessage(builder, listOf(details), alias)
        return Pair(builder, details)
    }

    private fun createParsedMessage(details: RequestedMessageDetails, index: Int = 0): Message =
        message().setMetadata(
            messageType = "Test",
            direction = details.storedMessage.direction.toGrpcDirection(),
            sessionAlias = details.storedMessage.sessionAlias,
            sequence = details.storedMessage.sequence,
            timestamp = details.storedMessage.timestamp,
        ).apply {
            metadataBuilder.idBuilder.apply {
                addSubsequence(index + 1)
                bookName = "test"
            }
        }.build()

    private fun notifyListeners(vararg messages: List<Message>) {
        val metadata = DeliveryMetadata("")
        listeners.forEach {
            it.handle(metadata, MessageGroupBatch.newBuilder()
                .apply {
                    for (messageGroup in messages) {
                        messageGroup.forEach(addGroupsBuilder()::plusAssign)
                    }
                }
                .build())
        }
    }
}