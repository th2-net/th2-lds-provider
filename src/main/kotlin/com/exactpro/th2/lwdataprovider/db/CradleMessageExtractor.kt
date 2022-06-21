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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.BookInfo
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.GroupedMessageFilterBuilder
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.resultset.CradleResultSet
import com.exactpro.th2.lwdataprovider.handlers.util.BookGroup
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
            .thenComparing<String> { it.sessionAlias }
            .thenComparingLong { it.sequence }
    }

    fun getBooks(): Collection<BookInfo> = storage.books

    fun getStreams(bookId: BookId): Collection<String> = storage.getSessionAliases(bookId)

    fun hasMessagesAfter(id: StoredMessageId): Boolean = hasMessages(id, TimeRelation.AFTER)

    fun hasMessagesBefore(id: StoredMessageId): Boolean = hasMessages(id, TimeRelation.BEFORE)

    fun hasMessagesInGroupAfter(group: String, bookId: BookId, lastGroupTimestamp: Instant): Boolean {
        GroupedMessageFilterBuilder()
            .bookId(bookId)
            .groupName(group)
            .limit(1)
            .timestampFrom().isGreaterThanOrEqualTo(lastGroupTimestamp)
            .build()
        val lastBatch: CradleResultSet<StoredGroupedMessageBatch> = storage.getGroupedMessageBatches(GroupedMessageFilterBuilder()
            .bookId(bookId)
            .groupName(group)
            .limit(1)
            .timestampFrom().isGreaterThanOrEqualTo(lastGroupTimestamp)
            .build())
        return lastBatch.hasNext() && lastBatch.next().run { firstTimestamp >= lastGroupTimestamp || lastTimestamp >= lastGroupTimestamp }
    }

    private fun hasMessages(id: StoredMessageId, order: TimeRelation): Boolean {
        val filter = when (order) {
            TimeRelation.AFTER -> MessageFilterBuilder.next(id, 1)
            TimeRelation.BEFORE -> MessageFilterBuilder.previous(id, 1)
        }.build()
        return storage.getMessages(filter).let {
            it.hasNext() && it.next().id != id
        }
    }

    fun getMessages(filter: MessageFilter, sink: MessageDataSink<String, StoredMessage>, measurement: DataMeasurement) {

        logger.info { "Executing query $filter" }
        val iterable = getMessagesFromCradle(measurement) { getMessages(filter) }
        for (storedMessage: StoredMessage in iterable) {

            sink.canceled?.apply {
                logger.info { "canceled because: $message" }
                return
            }

            sink.onNext(storedMessage.sessionAlias, storedMessage)
        }
    }


    fun getMessagesGroup(filter: GroupedMessageFilter, parameters: CradleGroupRequest, sink: MessageDataSink<String, StoredMessage>, measurement: DataMeasurement) {
        val group = filter.groupName
        val start = filter.from.value
        val end = filter.to.value
        val (
            sort: Boolean
        ) = parameters
        val iterator: Iterator<StoredGroupedMessageBatch> = storage.getGroupedMessageBatches(filter)
        if (!iterator.hasNext()) {
            logger.info { "Empty response received from cradle" }
            return
        }

        fun StoredMessage.timestampLess(batch: StoredGroupedMessageBatch): Boolean = timestamp < batch.firstTimestamp
        fun StoredGroupedMessageBatch.isNeedFiltration(): Boolean = firstTimestamp < start || lastTimestamp >= end
        fun StoredMessage.inRange(): Boolean = timestamp >= start && timestamp < end
        fun Collection<StoredMessage>.preFilter(): Collection<StoredMessage> =
            parameters.preFilter?.let { filter(it) } ?: this

        fun StoredGroupedMessageBatch.filterIfRequired(): Collection<StoredMessage> = if (isNeedFiltration()) {
            messages.filter(StoredMessage::inRange and parameters.preFilter)
        } else {
            messages.preFilter()
        }

        var prev: StoredGroupedMessageBatch? = null
        var currentBatch: StoredGroupedMessageBatch = iterator.next()
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

            sink.onNext(message.sessionAlias, message)

        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms" }

    }

    fun getMessagesWithSyncInterval(
        filters: List<MessageFilter>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
        measurement: DataMeasurement,
    ) {
        getGenericWithSyncInterval(
            filters,
            interval,
            sink,
            { it.timestamp },
            { it.sessionAlias },
        ) {
            getMessagesFromCradle(measurement) { getMessages(it) }.iterator()
        }
    }

    fun getGroupsWithSyncInterval(
        filters: Map<BookGroup, Pair<GroupedMessageFilter, CradleGroupRequest>>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
        measurement: DataMeasurement,
    ) {
        val filtersList = filters.toList()
        getGenericWithSyncInterval(
            filtersList,
            interval,
            BatchToMessageSinkAdapter(sink) { group, msg ->
                val (filter, param) = filters[BookGroup(group, msg.bookId)] ?: return@BatchToMessageSinkAdapter true
                msg.timestamp >= filter.from.value && msg.timestamp < filter.to.value && param.preFilter?.invoke(msg) != false
            },
            { it.firstTimestamp },
            { it.group },
        ) { (_, params) ->
            getMessagesFromCradle(measurement) { getGroupedMessageBatches(params.first) }.iterator()
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

    private fun <T> getMessagesFromCradle(dataMeasurement: DataMeasurement, supplier: CradleStorage.() -> Iterator<T>): Iterator<T> {
        val messages = dataMeasurement.start("cradle_messages_init").use { storage.supplier() }
        return StoredMessageIterator(messages.iterator(), dataMeasurement)
    }

}

private class BatchToMessageSinkAdapter(
    private val sink: MessageDataSink<String, StoredMessage>,
    private val preFilter: (String, StoredMessage) -> Boolean = { _, _ -> true },
) : MessageDataSink<String, StoredGroupedMessageBatch>, BaseDataSink by sink {
    override fun onNext(marker: String, data: StoredGroupedMessageBatch) {
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

private fun StoredGroupedMessageBatch.toShortInfo(): String = "${group}:${firstMessage.sequence}..${lastMessage.sequence} ($firstTimestamp..$lastTimestamp)"

data class CradleGroupRequest(
    val sort: Boolean,
    val preFilter: ((StoredMessage) -> Boolean)? = null,
)