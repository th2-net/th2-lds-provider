/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest.ResponseFormat.BASE_64
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest.ResponseFormat.PARSED
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest.ResponseFormat.ALL
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.google.protobuf.Timestamp
import java.time.Instant

class GrpcMessageProducer {

    companion object {

        fun createMessage(rawMessage: RequestedMessageDetails): MessageGroupResponse {
            val storedMessage = rawMessage.storedMessage
            val responseFormats = rawMessage.responseFormats

            return MessageGroupResponse.newBuilder().apply {
                messageId = convertMessageId(storedMessage.id)
                timestamp = convertTimestamp(storedMessage.timestamp)

                if (responseFormats.isEmpty() || responseFormats.contains(ALL) || responseFormats.contains(BASE_64)) {
                    bodyRaw = rawMessage.rawMessage?.body
                }
                if (responseFormats.isEmpty() || responseFormats.contains(ALL) || responseFormats.contains(PARSED)) {
                    rawMessage.parsedMessage?.forEach {
                        addMessageItem(MessageGroupItem.newBuilder().setMessage(it).build())
                    }
                }
            }.build()
        }

        private fun convertMessageId(messageID: StoredMessageId) : MessageID {
            return MessageID.newBuilder().also {
                it.connectionId = ConnectionID.newBuilder().setSessionAlias(messageID.streamName).build()
                it.direction = convertDirection(messageID)
                it.sequence = messageID.index
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