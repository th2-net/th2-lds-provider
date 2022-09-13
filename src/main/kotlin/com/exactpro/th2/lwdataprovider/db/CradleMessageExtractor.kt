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
import com.exactpro.cradle.TimeRelation
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.lwdataprovider.CradleMessageSource
import com.exactpro.th2.lwdataprovider.LOAD_MESSAGES_FROM_CRADLE_COUNTER
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.grpc.toRawMessage
import mu.KotlinLogging
import java.time.Instant
import java.util.LinkedList
import kotlin.concurrent.withLock
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(configuration: Configuration, private val cradleManager: CradleManager,
                             private val decoder: RabbitMqDecoder) {

    private val storage = cradleManager.storage
    private val batchSize = configuration.batchSize
    private val groupBufferSize = configuration.groupRequestBuffer
    private val codecUsePinAttributes = configuration.codecUsePinAttributes
    
    companion object {
        private val logger = KotlinLogging.logger { }
        private val STORED_MESSAGE_COMPARATOR: Comparator<StoredMessage> = Comparator.comparing<StoredMessage, Instant> { it.timestamp }
            .thenComparing({ it.direction }) { dir1, dir2 -> -(dir1.ordinal - dir2.ordinal) } // SECOND < FIRST
            .thenComparing<String> { it.streamName }
            .thenComparingLong { it.index }

        private val LOAD_MESSAGES_FROM_CRADLE_MESSAGE_COUNTER = LOAD_MESSAGES_FROM_CRADLE_COUNTER.labels(CradleMessageSource.MESSAGE.name)
        private val LOAD_MESSAGES_FROM_CRADLE_GROUP_COUNTER = LOAD_MESSAGES_FROM_CRADLE_COUNTER.labels(CradleMessageSource.GROUP.name)
    }

    fun getStreams(): Collection<String> = storage.streams

    fun getMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext, responseFormats: List<String>) {

        var msgCount = 0
        val time = measureTimeMillis {
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext)
            val sessionName = filter.streamName.value

            val builder = MessageGroupBatch.newBuilder()
            var msgBufferCount = 0
            val messageBuffer = ArrayList<RequestedMessageDetails>()

            var lastMsg: StoredMessage? = null
            for (storedMessage in iterable) {

                if (!requestContext.contextAlive) {
                    return
                }

                lastMsg = storedMessage
                val tmp = requestContext.createRequestAndAddToBatch(storedMessage, builder)
                messageBuffer.add(tmp)
                ++msgBufferCount

                if (msgBufferCount >= batchSize) {
                    val sendingTime = System.currentTimeMillis()
                    messageBuffer.forEach {
                        it.time = sendingTime
                        decoder.registerMessage(it)
                        requestContext.registerMessage(it)
                    }
                    if (codecUsePinAttributes) {
                        decoder.sendBatchMessage(builder.build(), sessionName)
                    } else {
                        decoder.sendAllBatchMessage(builder.build())
                    }

                    messageBuffer.clear()
                    builder.clear()
                    msgCount += msgBufferCount
                    logger.debug { "Message batch sent ($msgBufferCount). Total messages $msgCount" }
                    decoder.decodeBuffer.checkAndWait()
                    requestContext.checkAndWaitForRequestLimit(msgBufferCount)
                    msgBufferCount = 0
                }
            }

            if (msgBufferCount > 0) {
                if (codecUsePinAttributes) {
                    decoder.sendBatchMessage(builder.build(), sessionName)
                } else {
                    decoder.sendAllBatchMessage(builder.build())
                }

                val sendingTime = System.currentTimeMillis()
                messageBuffer.forEach {
                    it.time = sendingTime
                    decoder.registerMessage(it)
                    requestContext.registerMessage(it)
                }
                msgCount += msgBufferCount
            }

            requestContext.streamInfo.registerMessage(lastMsg?.id, lastMsg?.timestamp)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }

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

    private fun MessageRequestContext.createRequestAndAddToBatch(
        storedMessage: StoredMessage,
        builder: MessageGroupBatch.Builder
    ): RequestedMessageDetails {
        val id = storedMessage.id.toString()
        val decodingStep = startStep("decoding")
        val tmp = createMessageDetails(id, 0, storedMessage, emptyList()) { decodingStep.finish() }
        tmp.rawMessage = startStep("raw_message_parsing").use {
            storedMessage.toRawMessage()
        }.also {
            builder.addGroupsBuilder() += it
        }
        return tmp
    }

    private fun MessageRequestContext.createRequestAndSend(
        storedMessage: StoredMessage,
    ): RequestedMessageDetails {
        val id = storedMessage.id.toString()
        return createMessageDetails(id, 0, storedMessage, emptyList()).apply {
            rawMessage = startStep("raw_message_parsing").use {
                storedMessage.toRawMessage()
            }
            responseMessage()
            notifyMessage()
        }
    }

    fun getRawMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0
        val time = measureTimeMillis {
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext)

            val time = System.currentTimeMillis()
            var lastMsg: StoredMessage? = null
            for (storedMessage in iterable) {
                if (!requestContext.contextAlive) {
                    return
                }
                lastMsg = storedMessage
                val id = storedMessage.id.toString()
                val tmp = requestContext.createMessageDetails(id, time, storedMessage, emptyList())
                tmp.rawMessage = storedMessage.toRawMessage()

                tmp.responseMessage()
                msgCount++
            }
            requestContext.streamInfo.registerMessage(lastMsg?.id, lastMsg?.timestamp)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }


    fun getMessage(msgId: StoredMessageId, onlyRaw: Boolean, requestContext: MessageRequestContext) {

        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = storage.getMessage(msgId)

            if (message == null) {
                requestContext.writeErrorMessage("Message with id $msgId not found")
                requestContext.finishStream()
                return
            }

            val time = System.currentTimeMillis()
            val decodingStep = if (onlyRaw) null else requestContext.startStep("decoding")
            val tmp = requestContext.createMessageDetails(message.id.toString(), time, message, emptyList()) { decodingStep?.finish() }
            tmp.rawMessage = message.toRawMessage()
            requestContext.loadedMessages += 1

            if (onlyRaw) {
                tmp.responseMessage()
            } else {
                val msgBatch = MessageGroupBatch.newBuilder()
                    .apply { addGroupsBuilder() += tmp.rawMessage!! /*TODO: should be refactored*/ }
                    .build()
                decoder.registerMessage(tmp)
                requestContext.registerMessage(tmp)
                if (codecUsePinAttributes) {
                    decoder.sendBatchMessage(msgBatch, message.streamName)
                } else {
                    decoder.sendAllBatchMessage(msgBatch)
                }

            }

        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms"}

    }

    fun getMessagesGroup(group: String, parameters: CradleGroupRequest, requestContext: MessageRequestContext) {
        val (
            start: Instant,
            end: Instant,
            sort: Boolean,
            rawOnly: Boolean
        ) = parameters

        val messagesGroup: Iterable<StoredGroupMessageBatch> = cradleManager.storage.getGroupedMessageBatches(group, start, end)
            .toMetricIterable { storedGroup -> LOAD_MESSAGES_FROM_CRADLE_GROUP_COUNTER.inc(storedGroup.messageCount.toDouble()) }
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
        val batchBuilder = MessageGroupBatch.newBuilder()
        val detailsBuffer = arrayListOf<RequestedMessageDetails>()
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
                requestContext.updateLastMessage(group, buffer.last)
                tryDrain(group, buffer, detailsBuffer, batchBuilder, sort, requestContext, rawOnly)
            } else {
                generateSequence { if (!sort || remaining.peek()?.timestampLess(currentBatch) == true) remaining.poll() else null }.toCollection(buffer)

                val messageCount = prev.messageCount
                val prevRemaining = remaining.size
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

                val lastMsg = if (prevRemaining == remaining.size) {
                    buffer.last
                } else {
                    remaining.last
                }
                requestContext.updateLastMessage(group, lastMsg)
                tryDrain(group, buffer, detailsBuffer, batchBuilder, sort, requestContext, rawOnly)
            }
        }
        val remainingMessages = currentBatch.filterIfRequired()
        val lastMsg = remainingMessages.last()
        requestContext.updateLastMessage(group, lastMsg)

        if (prev == null) {
            // Only single batch was extracted
            drain(group, remainingMessages, detailsBuffer, batchBuilder, requestContext, rawOnly)
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
                detailsBuffer, batchBuilder, requestContext, rawOnly
            )
        }
    }

    private infix fun ((StoredMessage) -> Boolean).and(another: ((StoredMessage) -> Boolean)?): (StoredMessage) -> Boolean =
        another?.let {
            {
                this.invoke(it) && another.invoke(it)
            }
        } ?: this

    private fun MessageRequestContext.updateLastMessage(
        group: String,
        lastMsg: StoredMessage,
    ) {
        streamInfo.registerMessage(lastMsg.id, lastMsg.timestamp, group)
    }

    private fun tryDrain(
        group: String,
        buffer: LinkedList<StoredMessage>,
        detailsBuffer: MutableList<RequestedMessageDetails>,
        batchBuilder: MessageGroupBatch.Builder,
        sort: Boolean,
        requestContext: MessageRequestContext,
        rawOnly: Boolean,
    ) {
        if (buffer.size < batchSize && !rawOnly) {
            return
        }
        if (sort) {
            buffer.sortWith(STORED_MESSAGE_COMPARATOR)
        }
        if (rawOnly) {
            drain(group, buffer, detailsBuffer, batchBuilder, requestContext, true)
            buffer.clear() // we must pull all messages from the buffer
            return
        }
        val drainBuffer: MutableList<StoredMessage> = ArrayList(batchSize)
        while (buffer.size >= batchSize) {
            val sortedBatch = generateSequence(buffer::poll)
                .take(batchSize)
                .toCollection(drainBuffer)
            drain(group, sortedBatch, detailsBuffer, batchBuilder, requestContext, false)
            drainBuffer.clear()
        }
    }

    private fun drain(
        group: String,
        buffer: Collection<StoredMessage>,
        detailsBuffer: MutableList<RequestedMessageDetails>,
        batchBuilder: MessageGroupBatch.Builder,
        requestContext: MessageRequestContext,
        rawOnly: Boolean,
    ) {
        for (message in buffer) {
            if (rawOnly) {
                requestContext.createRequestAndSend(message)
                continue
            }
            detailsBuffer += requestContext.createRequestAndAddToBatch(message, batchBuilder)
            if (detailsBuffer.size == batchSize) {
                requestContext.sendBatch(group, batchBuilder, detailsBuffer)
            }
        }
        if (rawOnly) {
            return
        }
        requestContext.sendBatch(group, batchBuilder, detailsBuffer)
    }

    private fun MessageRequestContext.sendBatch(alias: String, builder: MessageGroupBatch.Builder, detailsBuf: MutableList<RequestedMessageDetails>) {
        if (detailsBuf.isEmpty()) {
            return
        }
        val messageCount = detailsBuf.size
        val sendingTime = System.currentTimeMillis()
        detailsBuf.forEach {
            it.time = sendingTime
            decoder.registerMessage(it)
            registerMessage(it)
        }
        decoder.sendBatchMessage(builder.build(), alias)
        builder.clear()
        detailsBuf.clear()
        decoder.decodeBuffer.checkAndWait()
        checkAndWaitForRequestLimit(messageCount)
    }

    private fun MessageRequestContext.checkAndWaitForRequestLimit(msgBufferCount: Int) {
        if (maxMessagesPerRequest > 0 && maxMessagesPerRequest <= messagesInProcess.addAndGet(msgBufferCount)) {
            startStep("await_queue").use {
                lock.withLock {
                    condition.await()
                }
            }
        }
    }

    private fun getMessagesFromCradle(filter: StoredMessageFilter, requestContext: MessageRequestContext): Iterable<StoredMessage> =
        requestContext.startStep("cradle").use {
            storage.getMessages(filter)
                .toMetricIterable{ LOAD_MESSAGES_FROM_CRADLE_MESSAGE_COUNTER.inc() }
        }
}

private fun StoredGroupMessageBatch.toShortInfo(): String = "${sessionGroup}:${firstMessage.index}..${lastMessage.index} ($firstTimestamp..$lastTimestamp)"

data class CradleGroupRequest(
    val start: Instant,
    val end: Instant,
    val sort: Boolean,
    val rawOnly: Boolean,
    val preFilter: ((StoredMessage) -> Boolean)? = null,
)