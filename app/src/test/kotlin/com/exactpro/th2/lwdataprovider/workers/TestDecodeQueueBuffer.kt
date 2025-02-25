/*
 * Copyright 2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.workers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.utils.TimeUtils
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue

class TestDecodeQueueBuffer {
    private val buffer = DecodeQueueBuffer()

    @Test
    fun `only expired requests are removed on timeout`() {
        val storedMessage = mock<StoredMessage> {
            on { id } doReturn StoredMessageId.fromString("test:sessionAlias:2:20201031010203123456789:1")
        }
        val onResponseExpired = mock<(RequestedMessageDetails) -> Unit>()
        val onResponseNotExpired = mock<(RequestedMessageDetails) -> Unit>()

        val currentTime = System.currentTimeMillis()
        val expired = RequestedMessageDetails(
            storedMessage = storedMessage,
            onResponse = onResponseExpired,
        ).apply {
            time = currentTime - 1000
        }
        val notExpired = RequestedMessageDetails(
            storedMessage = storedMessage,
            onResponse = onResponseNotExpired,
        ).apply {
            time = currentTime
        }
        buffer.add(expired, "test")
        buffer.add(notExpired, "test")

        val minTime = buffer.removeOlderThan(999)

        expect {
            that(expired) {
                get { completed }
                    .get { isDone }
                    .isTrue()

                get { transportParsedMessages }.isNull()
                get { protoParsedMessages }.isNull()
            }
            that(notExpired) {
                get { completed }
                    .get { isDone }
                    .isFalse()
            }

            that(minTime).isEqualTo(currentTime)

            catching {
                verify(onResponseExpired).invoke(same(expired))
            }.isSuccess()

            catching {
                verifyNoInteractions(onResponseNotExpired)
            }.isSuccess()
        }
    }

    @Test
    fun `remaining requests can be marked as done`() {
        val storedMessage = mock<StoredMessage> {
            on { id } doReturn StoredMessageId.fromString("test:sessionAlias:2:20201031010203123456789:1")
        }
        val onResponseExpired = mock<(RequestedMessageDetails) -> Unit>()
        val onResponseNotExpired = mock<(RequestedMessageDetails) -> Unit>()

        val currentTime = System.currentTimeMillis()
        val expired = RequestedMessageDetails(
            storedMessage = storedMessage,
            onResponse = onResponseExpired,
        ).apply {
            time = currentTime - 1000
        }
        val notExpired = RequestedMessageDetails(
            storedMessage = storedMessage,
            onResponse = onResponseNotExpired,
        ).apply {
            time = currentTime
        }
        buffer.add(expired, "test")
        buffer.add(notExpired, "test")

        buffer.removeOlderThan(999)

        expect {
            that(expired) {
                get { completed }
                    .get { isDone }
                    .isTrue()

                get { transportParsedMessages }.isNull()
                get { protoParsedMessages }.isNull()
            }
            that(notExpired) {
                get { completed }
                    .get { isDone }
                    .isFalse()
            }
        }


        buffer.responseTransportReceived(TransportRequestId(
            bookName = "test",
            messageID = MessageId.builder()
                .setDirection(Direction.OUTGOING)
                .setSessionAlias("sessionAlias")
                .setTimestamp(TimeUtils.fromIdTimestamp("20201031010203123456789"))
                .setSequence(1)
                .build()
        )) { listOf(ParsedMessage.EMPTY) }

        expect {
            that(notExpired) {
                get { completed }
                    .get { isDone }
                    .isTrue()
                get { transportParsedMessages }.isNotNull().containsExactly(ParsedMessage.EMPTY)
            }
            catching {
                verify(onResponseNotExpired).invoke(same(notExpired))
            }.isSuccess()
        }
    }
}