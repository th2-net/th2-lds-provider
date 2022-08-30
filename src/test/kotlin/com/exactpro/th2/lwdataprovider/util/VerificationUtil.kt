/*
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
 */

package com.exactpro.th2.lwdataprovider.util

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import org.junit.jupiter.api.Assertions

fun validateOrder(messages: List<StoredMessage>, expectedUniqueMessages: Int) {
    var prevMessage: StoredMessage? = null
    val ids = HashSet<StoredMessageId>(expectedUniqueMessages)
    for (message in messages) {
        ids += message.id
        prevMessage?.also {
            Assertions.assertTrue(it.timestamp <= message.timestamp) {
                "Unordered messages: $it and $message"
            }
        }
        prevMessage = message
    }
    Assertions.assertEquals(expectedUniqueMessages, ids.size) {
        "Unexpected number of IDs: $ids"
    }
}

fun validateMessagesOrder(messages: List<RequestedMessageDetails>, expectedUniqueMessages: Int) {
    var prevMessage: RequestedMessageDetails? = null
    val ids = HashSet<String>(expectedUniqueMessages)
    for (message in messages) {
        ids += message.id
        prevMessage?.also {
            Assertions.assertTrue(it.storedMessage.timestamp <= message.storedMessage.timestamp) {
                "Unordered messages: $it and $message"
            }
        }
        prevMessage = message
    }
    Assertions.assertEquals(expectedUniqueMessages, ids.size) {
        "Unexpected number of IDs: $ids"
    }
}