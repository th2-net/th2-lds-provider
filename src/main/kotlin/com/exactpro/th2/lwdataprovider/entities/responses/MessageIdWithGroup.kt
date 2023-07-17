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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.cradle.utils.EscapeUtils

class MessageIdWithGroup private constructor(
    val group: String?,
    val messageId: StoredMessageId,
) {

    companion object {
        @JvmStatic
        fun create(messageId: StoredMessageId): MessageIdWithGroup {
            return MessageIdWithGroup(null, messageId)
        }
        @JvmStatic
        fun create(group: String?, messageId: StoredMessageId): MessageIdWithGroup {
            return MessageIdWithGroup(group?.ifEmpty { messageId.sessionAlias }, messageId)
        }
        @JvmStatic
        fun fromString(value: String): MessageIdWithGroup {
            val parts = EscapeUtils.split(value).toMutableList()
            require(parts.size == 5 || parts.size == 6) {
                "invalid format for message ID with group: $value"
            }
            var group: String? = null
            if (parts.size == 6) {
                // book:<group>:alias:direction:timestamp:sequence
                group = parts[1]?.takeUnless { it.isEmpty() }
                parts.removeAt(1)
            }
            val seq = StoredMessageIdUtils.getSequence(parts)
            val timestamp = StoredMessageIdUtils.getTimestamp(parts)
            val direction = StoredMessageIdUtils.getDirection(parts)
            val session = StoredMessageIdUtils.getSessionAlias(parts)
            val book = StoredMessageIdUtils.getBookId(parts)
            val id = StoredMessageId(book, session, direction, timestamp, seq)
            return MessageIdWithGroup(group, id)
        }
    }
}