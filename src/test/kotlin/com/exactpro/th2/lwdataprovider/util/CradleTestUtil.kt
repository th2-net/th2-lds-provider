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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventToStore
import java.time.Instant

fun createCradleStoredMessage(
    streamName: String,
    direction: Direction,
    index: Long,
    content: String? = null
): StoredMessage = StoredMessage(
    MessageToStoreBuilder()
        .timestamp(Instant.now())
        .content(content?.toByteArray() ?: ByteArray(0))
        .build(),
    StoredMessageId(streamName, direction, index)
)

fun createEventToStore(
    eventId: String,
    start: Instant,
    end: Instant,
    parentEventId: StoredTestEventId? = null
): TestEventToStore = TestEventToStore().apply {
    id = StoredTestEventId(eventId)
    name = "test_event"
    type = "test"
    parentId = parentEventId
    content = ByteArray(0)
    isSuccess = true
    startTimestamp = start.plusSeconds(1)
    endTimestamp = end.minusSeconds(1)
}