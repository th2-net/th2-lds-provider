/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.*

@Deprecated("for 5.3 messages")
class MessageProducer53 {

    companion object {
        private val LOGGER_K = KotlinLogging.logger {  }

        fun createMessage(
            rawMessage: RequestedMessage,
            formatter: JsonFormatter?,
            includeRaw: Boolean,
        ): ProviderMessage53 {
            val convertToOneProtoMessage = rawMessage.protoMessage?.let { convertToOneProtoMessage(it) }
            val convertToOneTransportMessage = rawMessage.transportMessage?.let { convertToOnTransportMessage(it) }
            return ProviderMessage53(
                rawMessage.storedMessage,
                if (formatter == null) null else {
                    convertToOneProtoMessage?.let(formatter::print)
                        ?: convertToOneTransportMessage?.let {
                            formatter.print(rawMessage.sessionGroup, it)
                        }
                },
                if (includeRaw) {
                    rawMessage.storedMessage.let { Base64.getEncoder().encodeToString(it.content) }
                } else null,
                convertToOneProtoMessage?.metadata?.messageType ?: convertToOneTransportMessage?.type ?: ""
            )
        }

        private fun convertToOneProtoMessage(messages: List<Message>): Message {
            return when (messages.size) {
                1 -> messages[0]
                else -> messages[0].toBuilder().run {
                    messages.subList(1, messages.size).forEach { mergeFrom(it) }
                    metadataBuilder.messageType = messages.joinToString("/") { it.metadata.messageType }
                    clearFields()
                    addFields(mergeMessagesBody(messages))
                    build()
                }
            }
        }

        private fun convertToOnTransportMessage(messages: List<ParsedMessage>): ParsedMessage {
            return when (messages.size) {
                1 -> messages[0]
                else -> messages[0].also {
                    //FIXME: implement or migrate to MessageProducer
                    LOGGER_K.warn { "Returned only first message from ${messages.size} for ${it.id}" }
                }
            }
        }

        private fun isMessageTypeUnique(messages: List<Message>): Boolean {
            val messageTypes = messages.map { it.messageType }.toSet()
            return messageTypes.size == messages.size
        }

        private fun mergeMessagesBody(messages: List<Message>): Map<String, Message.Builder> {
            return if (isMessageTypeUnique(messages)) {
                messages.associate {
                    it.messageType to Message.newBuilder().addFields(it.fieldsMap)
                }
            } else {
                messages.associate {
                    val id = "${it.messageType}-${it.metadata.id.subsequenceList.joinToString("-")}"
                    id to Message.newBuilder().addFields(it.fieldsMap)
                }
            }
        }

    }

}