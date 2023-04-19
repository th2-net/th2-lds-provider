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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.transport.toProtoMessage
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.google.protobuf.Timestamp
import java.time.Instant

class GrpcMessageProducer {

    companion object {

        fun createMessage(rawMessage: RequestedMessage, responseFormats: Set<ResponseFormat>): MessageGroupResponse {
            val storedMessage = rawMessage.storedMessage

            return MessageGroupResponse.newBuilder().apply {
                messageId = convertMessageId(storedMessage.id)
                timestamp = convertTimestamp(storedMessage.timestamp)
                putAllMessageProperties(storedMessage.metadata?.toMap() ?: emptyMap())

                if (responseFormats.isEmpty() || ResponseFormat.BASE_64 in responseFormats) {
                    bodyRaw = rawMessage.rawMessage.value.body
                }
                if (responseFormats.isEmpty() || ResponseFormat.PROTO_PARSED in responseFormats) {
                    rawMessage.protoMessage?.forEach {
                        addMessageItem(MessageGroupItem.newBuilder().setMessage(it).build())
                    } ?: rawMessage.transportMessage?.forEach {
                        val book = rawMessage.requestId.bookName
                        val sessionGroup = rawMessage.sessionGroup
                        addMessageItem(MessageGroupItem.newBuilder().setMessage(it.toProtoMessage(book, sessionGroup)).build())
                    }
                }
            }.build()
        }

        private fun convertMessageId(messageID: StoredMessageId) : MessageID {
            return MessageID.newBuilder().also {
                it.connectionId = ConnectionID.newBuilder().setSessionAlias(messageID.sessionAlias).build()
                it.direction = convertDirection(messageID)
                it.sequence = messageID.sequence
                it.timestamp = messageID.timestamp.toTimestamp()
            }.build()
        }

        private fun convertDirection(messageID: StoredMessageId) : com.exactpro.th2.common.grpc.Direction {
            return if (messageID.direction == Direction.FIRST) {
                com.exactpro.th2.common.grpc.Direction.FIRST
            } else {
                com.exactpro.th2.common.grpc.Direction.SECOND
            }
        }

        private fun convertTimestamp(instant: Instant) : Timestamp {
            return Timestamp.newBuilder().setSeconds(instant.epochSecond).setNanos(instant.nano).build()
        }
    }

}