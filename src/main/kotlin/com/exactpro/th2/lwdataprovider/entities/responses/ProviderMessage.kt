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
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonRawValue
import java.time.Instant
import java.util.Collections

data class ProviderMessage(
    val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val parsedMessages: List<ProviderParsedMessage>,
    val rawMessageBase64: String?,

    @JsonIgnore
    val msgId: StoredMessageId
) : ResponseMessage {
    
    val id: String
        get() = msgId.toString()


    constructor(
        rawStoredMessage: StoredMessage,
        parsedMessages: List<ProviderParsedMessage> = Collections.emptyList(),
        base64Body: String?
    ) : this(
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        direction = Direction.fromStored(rawStoredMessage.direction ?: com.exactpro.cradle.Direction.FIRST),
        sessionId = rawStoredMessage.sessionAlias ?: "",
        parsedMessages = parsedMessages,
        rawMessageBase64 = base64Body,
        msgId = rawStoredMessage.id
    )
}

class ProviderParsedMessage(
    val id: String = "",
    @JsonRawValue
    val message: String = "{}"
) {

    companion object {

        private fun directionToString(dir: com.exactpro.th2.common.grpc.Direction) : String {
            return if (dir == com.exactpro.th2.common.grpc.Direction.FIRST) "first" else "second"
        }

        private fun subseqToString(subColl: Collection<Int>) : String {
            return if (subColl.isEmpty())
                ""
            else if (subColl.size == 1) {
                "." + subColl.iterator().next()
            } else  {
                val stringBuilder = StringBuilder()
                subColl.forEach { stringBuilder.append(".").append(it) }
                stringBuilder.toString()
            }
        }
        
        fun messageIdToString(msgId: MessageID) : String {
            return msgId.connectionId.sessionAlias + ":" + directionToString(msgId.direction) + ":" +
                    msgId.sequence + subseqToString(msgId.subsequenceList)
        }
    }
    
    constructor(msgId: MessageID, messageBody: String) : this (messageIdToString(msgId), messageBody)
}
