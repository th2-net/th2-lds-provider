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
import com.exactpro.cradle.messages.StoredMessageFilter
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RabbitMqDecoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import mu.KotlinLogging
import kotlin.concurrent.withLock
import kotlin.system.measureTimeMillis

class CradleMessageExtractor(configuration: Configuration, private val cradleManager: CradleManager,
                             private val decoder: RabbitMqDecoder) {

    private val storage = cradleManager.storage
    private val batchSize = configuration.batchSize
    
    companion object {
        private val logger = KotlinLogging.logger { }
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
                tmp.rawMessage = requestContext.startStep("raw_message_parsing").use { RawMessage.parseFrom(storedMessage.content) }.also {
                    builder.addGroupsBuilder() += it
                }

                if (msgBufferCount >= batchSize) {
                    requestContext.checkAndWaitForRequestLimit(msgBufferCount)
                    decoder.sendBatchMessage(builder, messageBuffer, sessionName)

                    messageBuffer.clear()
                    builder.clear()
                    msgCount += msgBufferCount
                    logger.debug { "Message batch sent ($msgBufferCount). Total messages $msgCount" }
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

    private fun getMessagesFromCradle(filter: StoredMessageFilter, requestContext: MessageRequestContext): Iterable<StoredMessage> =
        requestContext.startStep("cradle").use { storage.getMessages(filter) }

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

    private fun MessageRequestContext.sendBatch(alias: String, builder: MessageGroupBatch.Builder, detailsBuf: MutableList<RequestedMessageDetails>) {
        if (detailsBuf.isEmpty()) {
            return
        }
        val messageCount = detailsBuf.size
        checkAndWaitForRequestLimit(messageCount)
        decoder.sendBatchMessage(builder, detailsBuf, alias)
        builder.clear()
        detailsBuf.clear()
    }

    private fun getMessagesFromCradle(filter: StoredMessageFilter, requestContext: MessageRequestContext): Iterable<StoredMessage> =
        requestContext.startStep("cradle").use { storage.getMessages(filter) }
}

private fun StoredMessageBatch.toShortInfo(): String = "$streamName:${direction.label}:${firstMessage.index}..${lastMessage.index} ($firstTimestamp..$lastTimestamp)"
