/*******************************************************************************
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import mu.KotlinLogging

class ProtoCodecMessageListener(
    private val decodeQueue: RequestsBuffer,
) : MessageListener<MessageGroupBatch>  {
    
    override fun handle(deliveryMetadata: DeliveryMetadata, message: MessageGroupBatch) {
        message.groupsList.forEach { group ->
            if (group.messagesList.any { !it.hasMessage() }) {
                reportIncorrectGroup(group)
                return@forEach
            }
            val messageIdStr = group.messagesList.first().message.metadata.id.buildRequestId()

            decodeQueue.responseProtoReceived(messageIdStr) {
                group.messagesList.map { anyMsg -> anyMsg.message }
            }
        }
    }

    private fun MessageID.buildRequestId() : RequestId = ProtoRequestId(this)

    private fun reportIncorrectGroup(group: MessageGroup) {
        logger.error {
            "some messages in group are not parsed: ${
                group.messagesList.joinToString(",") {
                    "${it.kindCase} ${
                        when (it.kindCase) {
                            AnyMessage.KindCase.MESSAGE -> it.message.metadata.id.buildRequestId()
                            AnyMessage.KindCase.RAW_MESSAGE -> it.rawMessage.metadata.id.buildRequestId()
                            AnyMessage.KindCase.KIND_NOT_SET, null -> null
                        }
                    }"
                }
            }"
        }
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}
