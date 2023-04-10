/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import mu.KotlinLogging

class DemoCodecMessageListener(
    private val decodeQueue: RequestsBuffer,
) : MessageListener<DemoGroupBatch>  {

    override fun handle(deliveryMetadata: DeliveryMetadata, message: DemoGroupBatch) {

        message.groups.forEach { group ->
            if (group.messages.any { it !is DemoParsedMessage }) {
                reportIncorrectGroup(group)
                return@forEach
            }
            val messageIdStr = group.messages.first().id.buildMessageIdString()

            decodeQueue.responseDemoReceived(messageIdStr) {
                group.messages.map { anyMsg -> anyMsg as DemoParsedMessage }
            }
        }
    }

    private fun DemoMessageId.buildMessageIdString() : RequestId = DemoRequestId(this)

    private fun reportIncorrectGroup(group: DemoMessageGroup) {
        K_LOGGER.error {
            "some messages in group are not parsed: ${
                group.messages.joinToString(",") {
                    "${it::class.java} ${it.id.buildMessageIdString()}"
                }
            }"
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
    }
}
