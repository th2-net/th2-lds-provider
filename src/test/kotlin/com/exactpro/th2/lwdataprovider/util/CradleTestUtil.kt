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
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.TestEventToStore
import java.time.Instant

fun createCradleStoredMessage(
    streamName: String,
    direction: Direction,
    index: Long,
    content: String? = null,
    timestamp: Instant? = Instant.now(),
): StoredMessage = StoredMessage(
    MessageToStoreBuilder()
        .timestamp(timestamp)
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

fun createBatches(
    messagesPerBatch: Long,
    batchesCount: Int,
    overlapCount: Long,
    increase: Long,
    startTimestamp: Instant,
    end: Instant,
    aliasIndexOffset: Int = 0,
): List<StoredGroupMessageBatch> =
    ArrayList<StoredGroupMessageBatch>().apply {
        val startSeconds = startTimestamp.epochSecond
        repeat(batchesCount) {
            val start = Instant.ofEpochSecond(startSeconds + it * increase * (messagesPerBatch - overlapCount), startTimestamp.nano.toLong())
            add(StoredGroupMessageBatch().apply {
                createStoredMessages(
                    "test${it + aliasIndexOffset}",
                    Instant.now().run { epochSecond * 1_000_000_000 + nano },
                    start,
                    messagesPerBatch,
                    direction = if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                    incSeconds = increase,
                    end,
                ).forEach(this::addMessage)
            })
        }
    }

fun createStoredMessages(
    alias: String,
    startSequence: Long,
    startTimestamp: Instant,
    count: Long,
    direction: Direction = Direction.FIRST,
    incSeconds: Long = 10L,
    maxTimestamp: Instant,
): List<MessageToStore> {
    return (0 until count).map {
        val index = startSequence + it
        val instant = startTimestamp.plusSeconds(incSeconds * it).coerceAtMost(maxTimestamp)
        MessageToStoreBuilder()
            .direction(direction)
            .streamName(alias)
            .index(index)
            .timestamp(instant)
            .content(
                "abc".toByteArray()
            )
            .metadata("com.exactpro.th2.cradle.grpc.protocol", "abc")
            .build()
    }
}