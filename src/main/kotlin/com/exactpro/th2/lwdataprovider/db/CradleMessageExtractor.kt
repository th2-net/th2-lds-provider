/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.db

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.counters.Interval
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.GroupedMessageFilterBuilder
import com.exactpro.cradle.messages.MessageFilter
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.resultset.CradleResultSet
import com.exactpro.th2.lwdataprovider.db.GroupBatchCheckIterator.Companion.withCheck
import com.exactpro.th2.lwdataprovider.db.OrderStrategy.Companion.toOrderStrategy
import com.exactpro.th2.lwdataprovider.db.util.getGenericWithSyncInterval
import com.exactpro.th2.lwdataprovider.db.util.withMeasurements
import com.exactpro.th2.lwdataprovider.handlers.util.BookGroup
import com.exactpro.th2.lwdataprovider.toReportId
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(
    cradleManager: CradleManager,
    private val dataMeasurement: DataMeasurement,
) {

    private val storage: CradleStorage = cradleManager.storage

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun getAllGroups(bookId: BookId): Set<String> = measure("groups") { storage.getGroups(bookId) }.toSet()

    fun getGroups(bookId: BookId, from: Instant, to: Instant): Iterator<String> =
        storage.getSessionGroups(bookId, Interval(from, to))

    fun getAllStreams(bookId: BookId): Collection<String> = storage.getSessionAliases(bookId)

    fun getStreams(bookId: BookId, from: Instant, to: Instant): Iterator<String> {
        require(from.isBefore(to)) { "from '$from' must be before to '$to'" }
        return storage.getSessionAliases(bookId, Interval(from, to))
    }

    fun hasMessagesAfter(id: StoredMessageId): Boolean = hasMessages(id, TimeRelation.AFTER)

    fun hasMessagesBefore(id: StoredMessageId): Boolean = hasMessages(id, TimeRelation.BEFORE)

    fun hasMessagesInGroupAfter(group: String, bookId: BookId, lastGroupTimestamp: Instant): Boolean {
        val lastBatch: CradleResultSet<StoredGroupedMessageBatch> = storage.getGroupedMessageBatches(
            GroupedMessageFilterBuilder()
                .bookId(bookId)
                .groupName(group)
                .limit(1)
                .timestampFrom().isGreaterThanOrEqualTo(lastGroupTimestamp)
                .build()
        )
        return lastBatch.hasNext() && lastBatch.next()
            .run { firstTimestamp >= lastGroupTimestamp || lastTimestamp >= lastGroupTimestamp }
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

    fun getMessages(filter: MessageFilter, sink: MessageDataSink<String, StoredMessage>) {

        logger.info { "Executing query $filter" }
        val iterable = measure("init_messages") { storage.getMessages(filter) }
            .withMeasurements("messages", dataMeasurement)
        for (storedMessage: StoredMessage in iterable) {

            sink.canceled?.apply {
                logger.info { "canceled because: $message" }
                return
            }

            sink.onNext(storedMessage.sessionAlias, storedMessage)
        }
    }


    fun getMessagesGroup(
        filter: GroupedMessageFilter,
        parameters: CradleGroupRequest,
        sink: MessageDataSink<String, StoredMessage>
    ) {
        val group = filter.groupName
        val start = filter.from.value
        val end = filter.to.value
        val orderStrategy = filter.order?.toOrderStrategy() ?: OrderStrategy.DIRECT
        val iterator: Iterator<StoredGroupedMessageBatch> =
            measure("init_groups") { storage.getGroupedMessageBatches(filter) }
                .withCheck()
                .withMeasurements("groups", dataMeasurement)
        if (!iterator.hasNext()) {
            logger.info { "Empty response received from cradle" }
            return
        }

        fun StoredGroupedMessageBatch.isNeedFiltration(): Boolean = firstTimestamp < start || lastTimestamp >= end
        fun StoredMessage.inRange(): Boolean = timestamp >= start && timestamp < end
        fun Sequence<StoredMessage>.preFilter(): Sequence<StoredMessage> =
            parameters.preFilter?.let { filter(it) } ?: this

        fun Collection<StoredMessage>.preFilterTo(dest: MutableCollection<StoredMessage>): Collection<StoredMessage> {
            parameters.preFilter?.let { filterTo(dest, it) } ?: dest.addAll(this)
            return dest
        }

        fun StoredGroupedMessageBatch.filterIfRequired(): Collection<StoredMessage> = messages.asSequence().run {
            if (this@filterIfRequired.isNeedFiltration()) {
                filter(StoredMessage::inRange and parameters.preFilter)
            } else {
                preFilter()
            }
        }.toList()

        var prev: StoredGroupedMessageBatch? = null
        var currentBatch: StoredGroupedMessageBatch = iterator.next()
        val buffer: MutableList<StoredMessage> = ArrayList()
        while (iterator.hasNext()) {
            val measurement = dataMeasurement.start("process_cradle_group_batch")
            @Suppress("ConvertTryFinallyToUseCall")
            try {
                sink.canceled?.apply {
                    logger.info { "canceled because: $message" }
                    return
                }
                prev = currentBatch
                currentBatch = iterator.next()
                check(orderStrategy.checkBatchOrdered(prev, currentBatch)) {
                    "Unordered batches received for $orderStrategy: ${prev.toShortInfo()} and ${currentBatch.toShortInfo()}"
                }
                val needFiltration = prev.isNeedFiltration()

                if (orderStrategy.checkBatchOverlap(prev, currentBatch)) {
                    if (needFiltration) {
                        orderStrategy.reorder(prev.messages).filterTo(buffer, StoredMessage::inRange and parameters.preFilter)
                    } else {
                        orderStrategy.reorder(prev.messages).preFilterTo(buffer)
                    }
                    tryDrain(group, buffer, sink)
                } else {
                    orderStrategy.reorder(prev.messages).forEachIndexed { index, msg ->
                        if ((needFiltration && !msg.inRange()) || parameters.preFilter?.invoke(msg) == false) {
                            return@forEachIndexed
                        }
                        buffer += msg
                    }
                    tryDrain(group, buffer, sink)
                }
            } finally {
                measurement.close()
            }
        }
        val remainingMessages = orderStrategy.reorder(currentBatch.filterIfRequired())

        sink.canceled?.apply {
            logger.info { "canceled because: $message" }
            return
        }

        if (prev == null) {
            // Only single batch was extracted
            drain(group, remainingMessages, sink)
        } else {
            drain(
                group,
                ArrayList<StoredMessage>(buffer.size + remainingMessages.size).apply {
                    addAll(buffer)
                    addAll(remainingMessages)
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
        buffer: MutableList<StoredMessage>,
        sink: MessageDataSink<String, StoredMessage>,
    ) {
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

    fun getMessage(msgId: StoredMessageId, sink: MessageDataSink<String, StoredMessage>) {

        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = measure("single_message") { storage.getMessage(msgId) }

            if (message == null) {
                sink.onError("Message with id $msgId not found", msgId.toReportId())
                return
            }

            sink.onNext(message.sessionAlias, message)

        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms" }

    }

    fun getMessage(group: String, msgId: StoredMessageId, sink: MessageDataSink<String, StoredMessage>) {
        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId from group $group" }
            val batches = measure("group_message") {
                storage.getGroupedMessageBatches(
                    GroupedMessageFilter.builder()
                        .groupName(group)
                        .bookId(msgId.bookId)
                        .timestampTo().isLessThanOrEqualTo(msgId.timestamp)
                        .order(Order.REVERSE)
                        .build()
                )
            }

            if (batches != null && batches.hasNext()) {
                val batch = batches.next()
                val messages = batch.messages
                logger.debug { "Checking message in batch ${batch.group} (${messages.size} messages - ${batch.firstMessage.id}..${batch.lastMessage.id})" }
                messages.find { it.id == msgId }?.also {
                    logger.info { "Found message in batch (${batch.firstMessage.id}..${batch.lastMessage.id})" }
                    sink.onNext(group, it)
                    return@measureTimeMillis
                }
            } else {
                logger.info { "Empty response from cradle" }
            }
            sink.onError("Message with id $msgId not found", msgId.toReportId())
            logger.error { "Message with id $msgId was not found for group $group" }
            return // we have found nothing
        }

        logger.info { "Loaded 1 messages with id $msgId and group $group from DB $time ms" }
    }

    fun getMessagesWithSyncInterval(
        filters: List<MessageFilter>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
    ) {
        getGenericWithSyncInterval(
            logger,
            filters,
            interval,
            sink,
            { sink.onNext(it.sessionAlias, it) },
            { it.timestamp },
        ) {
            measure("init_messages") { storage.getMessages(it) }
                .withMeasurements("messages", dataMeasurement)
        }
    }

    fun getGroupsWithSyncInterval(
        filters: Map<BookGroup, Pair<GroupedMessageFilter, CradleGroupRequest>>,
        interval: Duration,
        sink: MessageDataSink<String, StoredMessage>,
    ) {
        val filtersList = filters.toList()
        getGenericWithSyncInterval(
            logger,
            filtersList,
            interval,
            sink,
            {
                val group = it.group
                for (msg in it.messages) {
                    val process = filters[BookGroup(group, msg.bookId)]?.let { (filter, param) ->
                        msg.timestamp >= filter.from.value && msg.timestamp < filter.to.value && param.preFilter?.invoke(
                            msg
                        ) != false
                    } ?: true
                    if (process) {
                        sink.onNext(group, msg)
                    }
                }
            },
            { it.firstTimestamp },
        ) { (_, params) ->
            measure("init_groups") { storage.getGroupedMessageBatches(params.first) }
                .withMeasurements("groups", dataMeasurement)
        }
    }

    private inline fun <T> measure(name: String, action: () -> T): T = dataMeasurement.start(name).use { action() }
}

internal class GroupBatchCheckIterator(
    private val original: Iterator<StoredGroupedMessageBatch>,
) : Iterator<StoredGroupedMessageBatch> by original {
    override fun next(): StoredGroupedMessageBatch = original.next().also { batch ->
        if (batch.messages.size > 1) {
            batch.messages.asSequence().chunked(2).forEach { (first, second) ->
                check(first.timestamp <= second.timestamp) {
                    "Unordered message received for: ${batch.toShortInfo()} batch, between ${first.toShortInfo()} and ${second.toShortInfo()} messages"
                }
            }
        }
    }

    companion object {
        fun Iterator<StoredGroupedMessageBatch>.withCheck(): Iterator<StoredGroupedMessageBatch> =
            GroupBatchCheckIterator(this)
    }
}

private fun StoredGroupedMessageBatch.toShortInfo(): String =
    "${group}:${firstMessage.sequence}..${lastMessage.sequence} ($firstTimestamp..$lastTimestamp)"

private fun StoredMessage.toShortInfo(): String =
    "${sessionAlias}:${sequence} ($timestamp)"

data class CradleGroupRequest(
    val preFilter: ((StoredMessage) -> Boolean)? = null,
)

private enum class OrderStrategy {
    DIRECT {
        /**
         * Batch order 0: [1, 2], 1: [2, 3], 2: [4, 5]
         */
        override fun checkBatchOrdered(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean =
            first.lastTimestamp <= second.firstTimestamp

        override fun checkMessageInOrderWithBatch(message: StoredMessage?, batch: StoredGroupedMessageBatch): Boolean =
            message?.timestampLess(batch) == true

        override fun checkBatchOverlap(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean =
            first.lastTimestamp < second.firstTimestamp

        override fun <T> reorder(collection: Collection<T>): Collection<T> = collection
    },
    REVERSE {
        /**
         * Batch order 0: [4, 5], 1: [2, 3], 2: [1, 2]
         */
        override fun checkBatchOrdered(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean =
            first.firstTimestamp >= second.lastTimestamp

        override fun checkMessageInOrderWithBatch(message: StoredMessage?, batch: StoredGroupedMessageBatch): Boolean =
            message?.timestampGreater(batch) == true

        override fun checkBatchOverlap(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean =
            first.firstTimestamp > second.lastTimestamp

        override fun <T> reorder(collection: Collection<T>): Collection<T> = collection.reversed()
    };

    /**
     * Check order of grouped batch. Batches should go one by one without overlapping
     */
    abstract fun checkBatchOrdered(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean
    abstract fun checkMessageInOrderWithBatch(message: StoredMessage?, batch: StoredGroupedMessageBatch): Boolean
    abstract fun checkBatchOverlap(first: StoredGroupedMessageBatch, second: StoredGroupedMessageBatch): Boolean

    abstract fun <T>reorder(collection: Collection<T>): Collection<T>

    companion object {
        internal fun Order.toOrderStrategy(): OrderStrategy = when(this) {
            Order.DIRECT -> DIRECT
            Order.REVERSE -> REVERSE
        }

        private fun StoredMessage.timestampLess(batch: StoredGroupedMessageBatch): Boolean = timestamp < batch.firstTimestamp
        private fun StoredMessage.timestampGreater(batch: StoredGroupedMessageBatch): Boolean = timestamp > batch.lastTimestamp
    }
}