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
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.LinkedList
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(
    private val groupBufferSize: Int,
    cradleManager: CradleManager,
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

    fun hasMessagesAfter(id: StoredMessageId, lastTimestamp: Instant): Boolean = hasMessages(id, lastTimestamp, TimeRelation.AFTER)

    fun hasMessagesBefore(id: StoredMessageId, lastTimestamp: Instant): Boolean = hasMessages(id, lastTimestamp, TimeRelation.BEFORE)

    fun hasMessagesInGroupAfter(group: String, lastGroupTimestamp: Instant): Boolean {
        val lastBatch: StoredGroupMessageBatch? = storage.getLastMessageBatchForGroup(group)
        return lastBatch != null && !lastBatch.isEmpty &&
                lastBatch.run { firstTimestamp >= lastGroupTimestamp || lastTimestamp >= lastGroupTimestamp }
    }

    private fun hasMessages(id: StoredMessageId, lastTimestamp: Instant, order: TimeRelation): Boolean {
        return storage.getNearestMessageId(id.streamName, id.direction, lastTimestamp, order).let {
            it != null && it != id
        }
    }

    fun getMessages(filter: StoredMessageFilter, sink: MessageDataSink<String, StoredMessage>, measurement: DataMeasurement) {

        logger.info { "Executing query $filter" }
        val iterable = getMessagesFromCradle(measurement) { getMessages(filter) }
        for (storedMessage: StoredMessage in iterable) {

            sink.canceled?.apply {
                logger.info { "canceled because: $message" }
                return
            }

            sink.onNext(storedMessage.streamName, storedMessage)
        }
    }


    fun getMessagesGroup(group: String, parameters: CradleGroupRequest, sink: MessageDataSink<String, StoredMessage>, measurement: DataMeasurement) {
        val (
            start: Instant,
            end: Instant,
            sort: Boolean
        ) = parameters
        val messagesGroup: MutableIterable<StoredGroupMessageBatch> = storage.getGroupedMessageBatches(group, start, end)
        val iterator = messagesGroup.iterator()
        if (!iterator.hasNext()) {
            logger.info { "Empty response received from cradle" }
            return
        }

        fun StoredMessage.timestampLess(batch: StoredGroupMessageBatch): Boolean = timestamp < batch.firstTimestamp
        fun StoredGroupMessageBatch.isNeedFiltration(): Boolean = firstTimestamp < start || lastTimestamp >= end
        fun StoredMessage.inRange(): Boolean = timestamp >= start && timestamp < end
        fun Collection<StoredMessage>.preFilter(): Collection<StoredMessage> =
            parameters.preFilter?.let { filter(it) } ?: this

        fun StoredGroupMessageBatch.filterIfRequired(): Collection<StoredMessage> = if (isNeedFiltration()) {
            messages.filter(StoredMessage::inRange and parameters.preFilter)
        } else {
            messages.preFilter()
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
                    prev.messages.filterTo(buffer, StoredMessage::inRange and parameters.preFilter)
                } else {
                    buffer.addAll(prev.messages.preFilter())
                }
                tryDrain(group, buffer, sort, sink)
            } else {
                generateSequence { if (!sort || remaining.peek()?.timestampLess(currentBatch) == true) remaining.poll() else null }.toCollection(buffer)

                val messageCount = prev.messageCount
                prev.messages.forEachIndexed { index, msg ->
                    if ((needFiltration && !msg.inRange()) || parameters.preFilter?.invoke(msg) == false) {
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

    private infix fun ((StoredMessage) -> Boolean).and(another: ((StoredMessage) -> Boolean)?): (StoredMessage) -> Boolean =
        another?.let {
            {
                this.invoke(it) && another.invoke(it)
            }
        } ?: this

    private fun tryDrain(
        group: String,
        buffer: LinkedList<StoredMessage>,
        sort: Boolean,
        sink: MessageDataSink<String, StoredMessage>,
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
        sink: MessageDataSink<String, StoredMessage>,
    ) {
        sink.onNext(group, buffer)
    }

    fun getMessage(msgId: StoredMessageId, sink: MessageDataSink<String, StoredMessage>, measurement: DataMeasurement) {

        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = measurement.start("cradle_message").use { storage.getMessage(msgId) }

            if (message == null) {
                sink.onError("Message with id $msgId not found")
                return
            }

            sink.onNext(message.streamName, message)

        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms" }

    }

    fun getMessagesWithSyncInterval(
        filters: List<StoredMessageFilter>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
        measurement: DataMeasurement,
    ) {
        getGenericWithSyncInterval(
            filters,
            interval,
            sink,
            { it.timestamp },
            { it.streamName },
        ) {
            getMessagesFromCradle(measurement) { getMessages(it) }.iterator()
        }
    }

    fun getGroupsWithSyncInterval(
        filters: Map<String, CradleGroupRequest>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
        measurement: DataMeasurement,
    ) {
        val filtersList = filters.toList()
        getGenericWithSyncInterval(
            filtersList,
            interval,
            BatchToMessageSinkAdapter(sink) { group, msg ->
                val (start, end, _, filter) = filters[group] ?: return@BatchToMessageSinkAdapter true
                msg.timestamp >= start && msg.timestamp < end && filter?.invoke(msg) != false
            },
            { it.firstTimestamp },
            { it.sessionGroup },
        ) { (group, params) ->
            getMessagesFromCradle(measurement) { getGroupedMessageBatches(group, params.start, params.end) }.iterator()
        }
    }

    private fun <F, D> getGenericWithSyncInterval(
        filters: List<F>,
        interval: Duration,
        sink: MessageDataSink<String, D>,
        timeExtractor: (D) -> Instant,
        markerExtractor: (D) -> String,
        sourceSupplier: (F) -> Iterator<D>,
    ) {
        require(!interval.isZero && !interval.isNegative) { "incorrect sync interval $interval" }

        fun durationInRange(first: D, second: D): Boolean {
            return Duration.between(timeExtractor(first), timeExtractor(second)).abs() < interval
        }
        fun MutableList<D?>.minMessage(): D? = asSequence().filterNotNull().minByOrNull { timeExtractor(it) }

        val cradleIterators: List<Iterator<D>> = filters.map { sourceSupplier(it) }
        val lastTimestamps: MutableList<D?> = cradleIterators.mapTo(arrayListOf()) { if (it.hasNext()) it.next() else null }
        var minTimestampMsg: D? = lastTimestamps.minMessage() ?: run {
            logger.debug { "all cradle iterators are empty" }
            return
        }

        do {
            var allDataLoaded = cradleIterators.none { it.hasNext() }
            cradleIterators.forEachIndexed { index, cradleIterator ->
                sink.canceled?.apply {
                    logger.info { "canceled the request: $message" }
                    return
                }
                var lastMessage: D? = lastTimestamps[index]?.also { msg ->
                    if (minTimestampMsg?.let { durationInRange(it, msg) } != false || allDataLoaded) {
                        sink.onNext(markerExtractor(msg), msg)
                        lastTimestamps[index] = null
                    } else {
                        // messages is not in range for now
                        return@forEachIndexed
                    }
                }
                var stop = false
                while (cradleIterator.hasNext() && !stop) {
                    sink.canceled?.apply {
                        logger.info { "canceled the request: $message" }
                        return
                    }
                    val message: D = cradleIterator.next()
                    if (minTimestampMsg?.let { durationInRange(it, message) } != false) {
                        sink.onNext(markerExtractor(message), message)
                    } else {
                        stop = true
                    }
                    lastMessage = message
                }
                if (cradleIterator.hasNext() || stop) {
                    lastTimestamps[index] = lastMessage
                    allDataLoaded = false
                }
            }
            minTimestampMsg = lastTimestamps.minMessage()
        } while (!allDataLoaded)
    }

    private fun <T> getMessagesFromCradle(dataMeasurement: DataMeasurement, supplier: CradleStorage.() -> Iterable<T>): Iterable<T> {
        val messages = dataMeasurement.start("cradle_messages_init").use { storage.supplier() }
        return object : Iterable<T> {
            override fun iterator(): Iterator<T> {
                return StoredMessageIterator(messages.iterator(), dataMeasurement)
            }
        }
    }

}

private class BatchToMessageSinkAdapter(
    private val sink: MessageDataSink<String, StoredMessage>,
    private val preFilter: (String, StoredMessage) -> Boolean = { _, _ -> true },
) : MessageDataSink<String, StoredGroupMessageBatch>, BaseDataSink by sink {
    override fun onNext(marker: String, data: StoredGroupMessageBatch) {
        for (message in data.messages) {
            if (preFilter(marker, message)) {
                sink.onNext(marker, message)
            }
        }
    }
}

private class StoredMessageIterator<T>(
    private val iterator: Iterator<T>,
    private val dataMeasurement: DataMeasurement,
) : Iterator<T> by iterator {
    override fun hasNext(): Boolean = dataMeasurement.start("cradle_messages").use { iterator.hasNext() }
}

private fun StoredGroupMessageBatch.toShortInfo(): String = "${sessionGroup}:${firstMessage.index}..${lastMessage.index} ($firstTimestamp..$lastTimestamp)"

data class CradleGroupRequest(
    val start: Instant,
    val end: Instant,
    val sort: Boolean,
    val preFilter: ((StoredMessage) -> Boolean)? = null,
)