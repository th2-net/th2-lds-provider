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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.PageId
import com.exactpro.cradle.PageInfo
import com.exactpro.cradle.messages.MessageToStoreBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.resultset.CradleResultSet
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.testevents.StoredTestEventSingle
import com.exactpro.cradle.testevents.TestEventSingleToStore
import com.exactpro.cradle.testevents.TestEventToStore
import java.time.Instant

fun createCradleStoredMessage(
    streamName: String,
    direction: Direction,
    index: Long,
    content: String = "hello",
    timestamp: Instant? = Instant.now(),
): StoredMessage = MessageToStoreBuilder()
    .bookId(BookId("test"))
    .direction(direction)
    .sessionAlias(streamName)
    .sequence(index)
    .timestamp(timestamp)
    .content(content.toByteArray())
    .metadata("com.exactpro.th2.cradle.grpc.protocol", "abc")
    .build()
    .let { msg ->
        StoredMessage(msg, msg.id, null)
    }

fun createPageInfo(
    pageName: String,
    started: Instant,
    ended: Instant,
    updated: Boolean = false,
    removed: Boolean = false,
): PageInfo = PageInfo(
    PageId(BookId("test"), pageName),
    started,
    ended,
    "test comment for $pageName",
    if (updated) started else null,
    if (removed) ended else null
)

fun createEventStoredEvent(
    eventId: String,
    start: Instant,
    end: Instant,
    parentEventId: StoredTestEventId? = null
): TestEventSingleToStore = TestEventToStore.singleBuilder()
    .id(BookId("test"), "test-scope", start.plusSeconds(1), eventId)
    .name("test_event")
    .type("test")
    .parentId(parentEventId)
    .content(ByteArray(0))
    .success(true)
    .endTimestamp(end.minusSeconds(1))
    .build()

fun TestEventSingleToStore.toStoredEvent(pageID: PageId? = null): StoredTestEventSingle = StoredTestEventSingle(this, pageID)

fun createEventId(id: String, book: String? = "test", scope: String? = "test-scope", timestamp: Instant? = Instant.now()): StoredTestEventId =
    StoredTestEventId(BookId(book), scope, timestamp, id)

class ListCradleResult<T>(collection: MutableCollection<T>) : CradleResultSet<T> {
    private val iterator: MutableIterator<T> = collection.iterator()
    override fun remove() = iterator.remove()

    override fun hasNext(): Boolean = iterator.hasNext()

    override fun next(): T = iterator.next()

}

class ImmutableListCradleResult<T>(collection: Collection<T>) : CradleResultSet<T> {
    private val iterator: Iterator<T> = collection.iterator()
    override fun remove() = throw UnsupportedOperationException()

    override fun hasNext(): Boolean = iterator.hasNext()

    override fun next(): T = iterator.next()
}

fun createBatches(
    messagesPerBatch: Long,
    batchesCount: Int,
    overlapCount: Long,
    increase: Long,
    startTimestamp: Instant,
    end: Instant,
    aliasIndexOffset: Int = 0,
): List<StoredGroupedMessageBatch> =
    ArrayList<StoredGroupedMessageBatch>().apply {
        val startSeconds = startTimestamp.epochSecond
        repeat(batchesCount) {
            val start = Instant.ofEpochSecond(startSeconds + it * increase * (messagesPerBatch - overlapCount), startTimestamp.nano.toLong())
            add(StoredGroupedMessageBatch(
                "test",
                createStoredMessages(
                    "test${it + aliasIndexOffset}",
                    Instant.now().run { epochSecond * 1_000_000_000 + nano },
                    start,
                    messagesPerBatch,
                    direction = if (it % 2 == 0) Direction.FIRST else Direction.SECOND,
                    incSeconds = increase,
                    end,
                ),
                PageId(BookId("test-book"), "test-page"),
                Instant.now(),
            ))
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
): List<StoredMessage> {
    return (0 until count).map {
        val index = startSequence + it
        val instant = startTimestamp.plusSeconds(incSeconds * it).coerceAtMost(maxTimestamp)
        createCradleStoredMessage(
            alias,
            direction,
            index,
            timestamp = instant,
        )
    }
}