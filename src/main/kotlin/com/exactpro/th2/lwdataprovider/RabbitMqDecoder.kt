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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.workers.CodecMessageListener
import com.exactpro.th2.lwdataprovider.workers.DecodeQueueBuffer
import com.exactpro.th2.lwdataprovider.workers.TimeoutChecker
import mu.KotlinLogging

class RabbitMqDecoder(
    configuration: Configuration,
    messageRouterParsedBatch: MessageRouter<MessageGroupBatch>,
    private val messageRouterRawBatch: MessageRouter<MessageGroupBatch>
) : TimeoutChecker, AutoCloseable {
    
    private val decodeBuffer = DecodeQueueBuffer(configuration.maxBufferDecodeQueue)
    private val parsedMonitor = messageRouterParsedBatch.subscribeAll(CodecMessageListener(decodeBuffer), QueueAttribute.PARSED.value, FROM_CODEC_ATTR)

    fun sendBatchMessage(batchBuilder: MessageGroupBatch.Builder, requests: Collection<RequestedMessageDetails>, session: String) {
        checkAndWaitFreeBuffer(requests.size)
        LOGGER.trace { "Sending batch with messages to codec. IDs: ${requests.joinToString { it.id }}" }
        val currentTimeMillis = System.currentTimeMillis()
        requests.forEach {
            onMessageRequest(it, batchBuilder, currentTimeMillis)
        }
        send(batchBuilder, session)
    }

    fun sendMessage(message: RequestedMessageDetails, session: String) {
        checkAndWaitFreeBuffer(1)
        LOGGER.trace { "Sending message to codec. ID: ${message.id}" }
        val builder = MessageGroupBatch.newBuilder()
        onMessageRequest(message, builder)
        send(builder, session)
    }

    override fun removeOlderThen(timeout: Long): Long {
        return decodeBuffer.removeOlderThan(timeout)
    }

    override fun close() {
        runCatching { parsedMonitor.unsubscribe() }
            .onFailure { LOGGER.error(it) { "Cannot unsubscribe from queue" } }
    }

    private fun checkAndWaitFreeBuffer(size: Int) {
        LOGGER.trace { "Checking if the decoding queue has free $size slot(s)" }
        decodeBuffer.checkAndWait(size)
    }

    private fun onMessageRequest(
        details: RequestedMessageDetails,
        batchBuilder: MessageGroupBatch.Builder,
        currentTimeMillis: Long = System.currentTimeMillis(),
    ) {
        details.time = currentTimeMillis
        registerMessage(details)
        details.readyToSend()
        val rawMessage = details.rawMessage ?: run {
            RawMessage.parseFrom(details.storedMessage.content).also {
                details.rawMessage = it
            }
        }
        batchBuilder.addGroupsBuilder() += rawMessage
    }

    private fun send(batchBuilder: MessageGroupBatch.Builder, session: String) {
        this.messageRouterRawBatch.send(batchBuilder.build(), session, QueueAttribute.RAW.value)
    }

    private fun registerMessage(message: RequestedMessageDetails) {
        this.decodeBuffer.add(message)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val FROM_CODEC_ATTR = "from_codec"
    }

}