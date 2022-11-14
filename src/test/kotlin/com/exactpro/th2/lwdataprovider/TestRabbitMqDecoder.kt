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
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.anyVararg
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.isNotNull
import strikt.assertions.isNull

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
    )

    @ParameterizedTest(name = "{0} message(s) in response")
    @ValueSource(ints = [1, 2])
    fun `notifies when response received`(messagesInResponse: Int) {
        val builder = MessageGroupBatch.newBuilder()
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = RequestedMessageDetails(
            createCradleStoredMessage("test", Direction.FIRST, 1),
            onResponse,
        )
        decoder.sendBatchMessage(builder, listOf(details), "test")

        verify(messageRouter).send(eq(builder.build()), anyVararg())
        val parsed = arrayListOf<Message>().apply {
            repeat(messagesInResponse) {
                this += message().setMetadata(
                    messageType = "Test",
                    direction = com.exactpro.th2.common.grpc.Direction.FIRST,
                    sessionAlias = "test",
                    sequence = 1,
                ).apply {
                    metadataBuilder.idBuilder.addSubsequence(it + 1)
                }.build()
            }
        }
        notifyListeners(parsed)
        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNotNull()
            .containsExactly(parsed)
    }

    @Test
    fun `notifies when timeout exceeded`() {
        val builder = MessageGroupBatch.newBuilder()
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = RequestedMessageDetails(
            createCradleStoredMessage("test", Direction.FIRST, 1),
            onResponse,
        )
        decoder.sendBatchMessage(builder, listOf(details), "test")

        verify(messageRouter).send(eq(builder.build()), anyVararg())

        decoder.removeOlderThen(0)

        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNull()
    }

    @Test
    fun `notifies on close`() {
        val builder = MessageGroupBatch.newBuilder()
        val onResponse = mock<(RequestedMessageDetails) -> Unit> { }
        val details = RequestedMessageDetails(
            createCradleStoredMessage("test", Direction.FIRST, 1),
            onResponse,
        )
        decoder.sendBatchMessage(builder, listOf(details), "test")

        verify(messageRouter).send(eq(builder.build()), anyVararg())

        decoder.close()

        verify(onResponse).invoke(same(details))
        expectThat(details).get { parsedMessage }.isNull()
    }

    private fun notifyListeners(vararg messages: List<Message>) {
        listeners.forEach {
            it.handler("", MessageGroupBatch.newBuilder()
                .apply {
                    for (messageGroup in messages) {
                        messageGroup.forEach(addGroupsBuilder()::plusAssign)
                    }
                }
                .build())
        }
    }
}