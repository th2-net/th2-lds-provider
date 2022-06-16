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

package com.exactpro.th2.lwdataprovider.db

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import mu.KotlinLogging
import java.time.Instant
import java.util.LinkedList
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(
    private val groupBufferSize: Int,
    cradleManager: CradleManager
) {

    private val storage: CradleStorage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
        private val STORED_MESSAGE_COMPARATOR: Comparator<StoredMessage> = Comparator.comparing<StoredMessage, Instant> { it.timestamp }
            .thenComparing({ it.direction }) { dir1, dir2 -> -(dir1.ordinal - dir2.ordinal) } // SECOND < FIRST
            .thenComparing<String> { it.streamName }
            .thenComparingLong { it.index }
    }

    fun getStreams(): Collection<String> = storage.streams

    fun getMessages(filter: StoredMessageFilter, sink: DataSink<StoredMessage>, measurement: DataMeasurement) {

        logger.info { "Executing query $filter" }
        val iterable = getMessagesFromCradle(filter, measurement);
        for (storedMessage: StoredMessage in iterable) {

            sink.canceled?.apply {
                logger.info { "canceled because: $message" }
                return
            }

            sink.onNext(storedMessage)
        }
    }


    fun getMessagesGroup(group: String, start: Instant, end: Instant, sort: Boolean, sink: DataSink<StoredMessage>, measurement: DataMeasurement) {
        val messagesGroup: MutableIterable<StoredGroupMessageBatch> = storage.getGroupedMessageBatches(group, start, end)
        val iterator = messagesGroup.iterator()
        if (!iterator.hasNext()) {
            return
        }

        fun StoredMessage.timestampLess(batch: StoredGroupMessageBatch): Boolean = timestamp < batch.firstTimestamp
        fun StoredGroupMessageBatch.isNeedFiltration(): Boolean = firstTimestamp < start || lastTimestamp > end
        fun StoredMessage.inRange(): Boolean = timestamp >= start && timestamp <= end
        fun StoredGroupMessageBatch.filterIfRequired(): Collection<StoredMessage> = if (isNeedFiltration()) {
            messages.filter(StoredMessage::inRange)
        } else {
            messages
        }

        var prev: StoredGroupMessageBatch? = null
        var currentBatch: StoredGroupMessageBatch = iterator.next()
        val buffer: LinkedList<StoredMessage> = LinkedList()
        val remaining: LinkedList<StoredMessage> = LinkedList()
        while (iterator.hasNext()) {
            prev = currentBatch
            currentBatch = iterator.next()
            check(prev.firstTimestamp <= currentBatch.firstTimestamp) {
                "Unordered batches received: ${prev.toShortInfo()} and ${currentBatch.toShortInfo()}"
            }
            val needFiltration = prev.isNeedFiltration()

            if (prev.lastTimestamp < currentBatch.firstTimestamp) {
                if (needFiltration) {
                    prev.messages.filterTo(buffer, StoredMessage::inRange)
                } else {
                    buffer.addAll(prev.messages)
                }
                tryDrain(group, buffer, sort, sink)
            } else {
                generateSequence { if (!sort || remaining.peek()?.timestampLess(currentBatch) == true) remaining.poll() else null }.toCollection(buffer)

                val messageCount = prev.messageCount
                prev.messages.forEachIndexed { index, msg ->
                    if (needFiltration && !msg.inRange()) {
                        return@forEachIndexed
                    }
                    if (!sort || msg.timestampLess(currentBatch)) {
                        buffer += msg
                    } else {
                        check(remaining.size < groupBufferSize) {
                            "the group buffer size cannot hold all messages: current size $groupBufferSize but needs ${messageCount - index} more"
                        }
                        remaining += msg
                    }
                }

                tryDrain(group, buffer, sort, sink)
            }
        }
        val remainingMessages = currentBatch.filterIfRequired()

        if (prev == null) {
            // Only single batch was extracted
            drain(group, remainingMessages, sink)
        } else {
            drain(
                group,
                ArrayList<StoredMessage>(buffer.size + remaining.size + remainingMessages.size).apply {
                    addAll(buffer)
                    addAll(remaining)
                    addAll(remainingMessages)
                    if (sort) {
                        sortWith(STORED_MESSAGE_COMPARATOR)
                    }
                },
                sink
            )
        }
    }

    private fun tryDrain(
        group: String,
        buffer: LinkedList<StoredMessage>,
        sort: Boolean,
        sink: DataSink<StoredMessage>,
    ) {
        if (sort) {
            buffer.sortWith(STORED_MESSAGE_COMPARATOR)
        }
        drain(group, buffer, sink)
        buffer.clear()
    }

    private fun drain(
        group: String,
        buffer: Collection<StoredMessage>,
        sink: DataSink<StoredMessage>,
    ) {
        sink.onNext(buffer)
    }

    fun getMessage(msgId: StoredMessageId, sink: DataSink<StoredMessage>, measurement: DataMeasurement) {

        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = measurement.start("cradle_message").use { storage.getMessage(msgId) }

            if (message == null) {
                sink.onError("Message with id $msgId not found")
                return
            }

            sink.onNext(message)

        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms" }

    }

    private fun getMessagesFromCradle(filter: StoredMessageFilter, dataMeasurement: DataMeasurement): Iterable<StoredMessage> {
        val messages = dataMeasurement.start("cradle_messages_init").use { storage.getMessages(filter) }
        return object : Iterable<StoredMessage> {
            override fun iterator(): Iterator<StoredMessage> {
                return StoredMessageIterator(messages.iterator(), dataMeasurement)
            }
        }
    }

}

private class StoredMessageIterator(
    private val iterator: MutableIterator<StoredMessage>,
    private val dataMeasurement: DataMeasurement
) : Iterator<StoredMessage> by iterator {
    override fun hasNext(): Boolean = dataMeasurement.start("cradle_messages").use { iterator.hasNext() }
}

private fun StoredGroupMessageBatch.toShortInfo(): String = "${sessionGroup}:${firstMessage.index}..${lastMessage.index} ($firstTimestamp..$lastTimestamp)"