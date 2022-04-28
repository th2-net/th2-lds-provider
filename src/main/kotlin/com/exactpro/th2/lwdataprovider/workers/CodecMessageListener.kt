/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.workers

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.MessageListener
import mu.KotlinLogging
import java.util.Collections

class CodecMessageListener(
    private val decodeQueue: RequestsBuffer
) : MessageListener<MessageGroupBatch>  {
    
    private fun buildMessageIdString(messageId: MessageID) : String {
        return messageId.connectionId.sessionAlias + ":" + 
                (if (messageId.direction == Direction.FIRST) "first" else "second") + ":" + messageId.sequence
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    override fun handler(consumerTag: String, message: MessageGroupBatch) {

        message.groupsList.forEach { group ->
            if (group.messagesList.any { !it.hasMessage() }) {
                reportIncorrectGroup(group)
                return@forEach
            }
            val messageIdStr = buildMessageIdString(group.messagesList.first().message.metadata.id)

            decodeQueue.responseReceived(messageIdStr) {
                group.messagesList.map { anyMsg -> anyMsg.message }
            }
        }
    }

    private fun reportIncorrectGroup(group: MessageGroup) {
        logger.error {
            "some messages in group are not parsed: ${
                group.messagesList.joinToString(",") {
                    "${it.kindCase} ${
                        when (it.kindCase) {
                            AnyMessage.KindCase.MESSAGE -> buildMessageIdString(it.message.metadata.id)
                            AnyMessage.KindCase.RAW_MESSAGE -> buildMessageIdString(it.rawMessage.metadata.id)
                            AnyMessage.KindCase.KIND_NOT_SET, null -> null
                        }
                    }"
                }
            }"
        }
    }
}
