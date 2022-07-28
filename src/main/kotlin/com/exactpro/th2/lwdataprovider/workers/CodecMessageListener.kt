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
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.MessageListener
import mu.KotlinLogging
import java.util.Collections

class CodecMessageListener(
    private val decodeQueue: DecodeQueueBuffer
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

            val msgIdQueue = decodeQueue.removeById(messageIdStr)
            if (msgIdQueue != null) {
                logger.debug { "Received message from codec $messageIdStr. Count of awaiters: ${msgIdQueue.size}. Messages count: ${group.messagesCount}" }

                msgIdQueue.forEach {
                    it.parsedMessage = group.messagesList.map { anyMsg -> anyMsg.message }
                    it.responseMessage()
                    it.notifyMessage()
                }
            } else {
                logger.debug { "Not found expected from codec message with id $messageIdStr" }
            }
        }

        decodeQueue.checkAndUnlock()
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

    class MessageIterator(private val srcIterator: Iterator<Message>) : Iterator<List<Message>> {
        
        private var currElement: Ent? = null
        
        companion object {
            
            fun iterable(it: Iterable<Message>) : Iterable<List<Message>> 
                = Iterable { MessageIterator(it.iterator()) }

            private fun compareMsgIds(msgID: MessageID, other: MessageID) : Boolean {
                return msgID.connectionId == other.connectionId && msgID.direction == other.direction
                        && msgID.sequence == other.sequence;
            }
            
            private class Ent {
                var currElements: MutableList<Message> = Collections.emptyList()
                var id: MessageID = MessageID.getDefaultInstance()
                
                constructor() {
                    currElements = Collections.emptyList()
                    id = MessageID.getDefaultInstance()
                }

                constructor(msg: Message) {
                    currElements = Collections.singletonList(msg)
                    id  = msg.metadata.id
                }
                
                fun add(msg: Message) {
                    if (currElements.isEmpty()) {
                        currElements = Collections.singletonList(msg)
                        id = msg.metadata.id
                    } else if (currElements.size == 1) {
                        currElements = ArrayList(currElements)
                        currElements.add(msg)
                    } else {
                        currElements.add(msg)
                    }
                }
            }
        }
        
        override fun hasNext(): Boolean {
            return this.srcIterator.hasNext() || currElement != null
        }
        
        override fun next(): List<Message> {
            while (srcIterator.hasNext()) {
                val msg = srcIterator.next()
                if (currElement == null) {
                    currElement = Ent(msg)
                } else {
                    val tmpEl = currElement!!
                    if (!compareMsgIds(msg.metadata.id, tmpEl.id)) {
                        this.currElement = Ent(msg)
                        return tmpEl.currElements
                    } else {
                        tmpEl.add(msg)
                    }
                }
            }

            currElement?.let {
                currElement = null
                return it.currElements
            } ?: throw NoSuchElementException()
            
        }
    }
}
