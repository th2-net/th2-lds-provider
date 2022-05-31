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
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageBatch
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
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
    
    companion object {
        private val logger = KotlinLogging.logger { }
        private val STORED_MESSAGE_COMPARATOR: Comparator<StoredMessage> = Comparator.comparing<StoredMessage, Instant> { it.timestamp }
            .thenComparing({ it.direction }) { dir1, dir2 -> -(dir1.ordinal - dir2.ordinal) } // SECOND < FIRST
            .thenComparing<String> { it.streamName }
            .thenComparingLong { it.index }
    }

    fun getStreams(): Collection<String> = storage.streams
    
    fun getMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0
        val time = measureTimeMillis { 
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext);
            val sessionName = filter.streamName.value

            val builder = MessageGroupBatch.newBuilder()
            var msgBufferCount = 0
            val messageBuffer = ArrayList<RequestedMessageDetails>()

            var msgId: StoredMessageId? = null
            for (storedMessage in iterable) {

                if (!requestContext.contextAlive) {
                    return;
                }

                msgId = storedMessage.id
                val tmp = requestContext.createRequest(storedMessage)
                messageBuffer.add(tmp)
                ++msgBufferCount

                if (msgBufferCount >= batchSize) {
                    // TODO: requestContext.registerMessage(it) in for messages
                    decoder.sendBatchMessage(builder, messageBuffer, sessionName)

                    messageBuffer.clear()
                    builder.clear()
                    msgCount += msgBufferCount
                    logger.debug { "Message batch sent ($msgBufferCount). Total messages $msgCount" }
                    requestContext.checkAndWaitForRequestLimit(msgBufferCount)
                    msgBufferCount = 0
                }
            }
            
            if (msgBufferCount > 0) {
                decoder.sendBatchMessage(builder, messageBuffer, sessionName)
                msgCount += msgBufferCount
            }

            requestContext.streamInfo.registerMessage(msgId)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }

    private fun MessageRequestContext.createRequest(
        storedMessage: StoredMessage
    ): RequestedMessageDetails {
        val id = storedMessage.id.toString()
        val decodingStep = startStep("decoding")
        return createMessageDetails(id, storedMessage) { decodingStep.finish() }
    }

    private fun MessageRequestContext.createRequestAndSend(
        storedMessage: StoredMessage,
    ): RequestedMessageDetails {
        val id = storedMessage.id.toString()
        return createMessageDetails(id, storedMessage).apply {
            rawMessage = startStep("raw_message_parsing").use { RawMessage.parseFrom(storedMessage.content) }
            responseMessage()
        }
    }

    fun getRawMessages(filter: StoredMessageFilter, requestContext: MessageRequestContext) {

        var msgCount = 0
        val time = measureTimeMillis {
            logger.info { "Executing query $filter" }
            val iterable = getMessagesFromCradle(filter, requestContext);

            val time = System.currentTimeMillis()
            var msgId: StoredMessageId? = null
            for (storedMessageBatch in iterable) {
                if (!requestContext.contextAlive) {
                    return;
                }
                msgId = storedMessageBatch.id
                val id = storedMessageBatch.id.toString()
                val tmp = requestContext.createMessageDetails(id, storedMessageBatch)
                tmp.rawMessage = RawMessage.parseFrom(storedMessageBatch.content)
                tmp.responseMessage()
                msgCount++
            }
            requestContext.streamInfo.registerMessage(msgId)
            requestContext.loadedMessages += msgCount
        }

        logger.info { "Loaded $msgCount messages from DB $time ms"}

    }


    fun getMessage(msgId: StoredMessageId, onlyRaw: Boolean, requestContext: MessageRequestContext) {
        
        val time = measureTimeMillis {
            logger.info { "Extracting message: $msgId" }
            val message = storage.getMessage(msgId);
            
            if (message == null) {
                requestContext.writeErrorMessage("Message with id $msgId not found")
                requestContext.finishStream()
                return
            }

            val decodingStep = if (onlyRaw) null else requestContext.startStep("decoding")
            val tmp = requestContext.createMessageDetails(message.id.toString(), message) { decodingStep?.finish() }
            tmp.rawMessage = RawMessage.parseFrom(message.content)
            requestContext.loadedMessages += 1
            
            if (onlyRaw) {
                tmp.responseMessage()
            } else {
                decoder.sendMessage(tmp, message.streamName)
            }
            
        }

        logger.info { "Loaded 1 messages with id $msgId from DB $time ms"}

    }

    fun getMessagesGroup(group: String, start: Instant, end: Instant, sort: Boolean, rawOnly: Boolean, requestContext: MessageRequestContext) {
        val messagesGroup = cradleManager.storage.getGroupedMessageBatches(group, start, end)
        val iterator = messagesGroup.iterator()
        var prev: StoredMessageBatch? = null
        if (!iterator.hasNext()) {
            return
        }

        fun StoredMessage.timestampLess(batch: StoredMessageBatch): Boolean = timestamp < batch.firstTimestamp
        fun StoredMessageBatch.isNeedFiltration(): Boolean = firstTimestamp < start || lastTimestamp > end
        fun StoredMessage.inRange(): Boolean = timestamp >= start && timestamp <= end
        fun StoredMessageBatch.filterIfRequired(): Collection<StoredMessage> = if (isNeedFiltration()) {
            messages.filter(StoredMessage::inRange)
        } else {
            messages
        }

        var currentBatch: StoredMessageBatch = iterator.next()
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
                    prev.messages.filterTo(buffer, StoredMessage::inRange)
                } else {
                    buffer.addAll(prev.messages)
                }
                requestContext.streamInfo.registerMessage(buffer.last.id)
                tryDrain(group, buffer, detailsBuffer, batchBuilder, sort, requestContext, rawOnly)
            } else {
                generateSequence { if (!sort || remaining.peek()?.timestampLess(currentBatch) == true) remaining.poll() else null }.toCollection(buffer)

                val messageCount = prev.messageCount
                val prevRemaining = remaining.size
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

                val lastId = if (prevRemaining == remaining.size) {
                    buffer.last
                } else {
                    remaining.last
                }.id
                requestContext.streamInfo.registerMessage(lastId)
                tryDrain(group, buffer, detailsBuffer, batchBuilder, sort, requestContext, rawOnly)
            }
        }
        val remainingMessages = currentBatch.filterIfRequired()
        val lastMsgId = remainingMessages.last().id
        requestContext.streamInfo.registerMessage(lastMsgId)

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
            detailsBuffer += requestContext.createRequest(message)
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
        // TODO: registerMessage(it) in message
        decoder.sendBatchMessage(builder, detailsBuf, alias)
        builder.clear()
        detailsBuf.clear()
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
        requestContext.startStep("cradle").use { storage.getMessages(filter) }
}

private fun StoredMessageBatch.toShortInfo(): String = "$streamName:${direction.label}:${firstMessage.index}..${lastMessage.index} ($firstTimestamp..$lastTimestamp)"
