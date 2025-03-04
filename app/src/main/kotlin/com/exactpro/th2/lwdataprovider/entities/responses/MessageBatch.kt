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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import java.time.Instant

data class MessageWrapper(
    val id: StoredMessageId,
    val message: StoredMessage,
    private val result: ParsedMessageBatch
) {
    companion object {
        fun build(message: StoredMessage, parseMessageBatch: ParsedMessageBatch): MessageWrapper {
            return MessageWrapper(
                id = message.id,
                message = message,
                result = parseMessageBatch
            )
        }
    }

    fun getParsedMessage(): ProviderMessage {
        val parsedBatch = result
        return  parsedBatch.batch.getValue(id)
    }
}


data class MessageBatch(
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val id: StoredMessageId,
    val batch: Collection<StoredMessage>
) {
    companion object {
        fun build(batch: Collection<StoredMessage>): MessageBatch {
            return MessageBatch(
                startTimestamp = batch.first().timestamp,
                endTimestamp = batch.last().timestamp,
                id = batch.first().id,
                batch = batch
            )
        }
    }
}


data class ParsedMessageBatch(
    val id: StoredMessageId,
    val batch: Map<StoredMessageId, ProviderMessage>,
    val attachedEvents: Boolean
)

