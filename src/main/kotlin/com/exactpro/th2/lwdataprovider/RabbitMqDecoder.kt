/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup
import com.exactpro.th2.lwdataprovider.workers.CodecMessageListener
import com.exactpro.th2.lwdataprovider.workers.DecodeQueueBuffer
import com.exactpro.th2.lwdataprovider.workers.DemoCodecMessageListener
import com.exactpro.th2.lwdataprovider.workers.TimeoutChecker
import mu.KotlinLogging

class RabbitMqDecoder(
    private val messageRouterRawBatch: MessageRouter<MessageGroupBatch>,
    private val messageRouterDemoBatch: MessageRouter<DemoGroupBatch>,
    maxDecodeQueue: Int,
    private val codecUsePinAttributes: Boolean,
) : TimeoutChecker, Decoder, AutoCloseable {
    
    private val decodeBuffer = DecodeQueueBuffer(maxDecodeQueue)
    private val parsedMonitor = messageRouterRawBatch.subscribeAll(CodecMessageListener(decodeBuffer), QueueAttribute.PARSED.value, FROM_CODEC_ATTR)
    private val demoMonitor = messageRouterDemoBatch.subscribeAll(DemoCodecMessageListener(decodeBuffer), "demo", FROM_CODEC_ATTR)

    override fun sendBatchMessage(batchBuilder: MessageGroupBatch.Builder, requests: Collection<RequestedMessageDetails>, session: String) {
        checkAndWaitFreeBuffer(requests.size)
        LOGGER.trace { "Sending proto batch with messages to codec. IDs: ${requests.joinToString { it.id }}" }
        val currentTimeMillis = System.currentTimeMillis()
        requests.forEach {
            onMessageRequest(it, batchBuilder, session, currentTimeMillis)
        }
        send(batchBuilder, session)
    }
    override fun sendBatchMessage(
        batch: DemoGroupBatch,
        requests: Collection<RequestedMessageDetails>,
        session: String
    ) {
        checkAndWaitFreeBuffer(requests.size)
        LOGGER.trace { "Sending demo batch with messages to codec. IDs: ${requests.joinToString { it.id }}" }
        val currentTimeMillis = System.currentTimeMillis()
        requests.forEach {
            onMessageRequest(it, batch, session, currentTimeMillis)
        }
        send(batch, session)
    }

    override fun sendMessage(message: RequestedMessageDetails, session: String) {
        checkAndWaitFreeBuffer(1)
        LOGGER.trace { "Sending message to codec. ID: ${message.id}" }
        val builder = MessageGroupBatch.newBuilder()
        onMessageRequest(message, builder, session)
        send(builder, session)
    }

    override fun removeOlderThen(timeout: Long): Long {
        return decodeBuffer.removeOlderThan(timeout)
    }

    override fun close() {
        runCatching { parsedMonitor.unsubscribe() }
            .onFailure { LOGGER.error(it) { "Cannot unsubscribe from parsed queue" } }
        runCatching { demoMonitor.unsubscribe() }
            .onFailure { LOGGER.error(it) { "Cannot unsubscribe from demo queue" } }
        decodeBuffer.close()
    }

    private fun checkAndWaitFreeBuffer(size: Int) {
        LOGGER.trace { "Checking if the decoding queue has free $size slot(s)" }
        decodeBuffer.checkAndWait(size)
    }

    private fun onMessageRequest(
        details: RequestedMessageDetails,
        batchBuilder: MessageGroupBatch.Builder,
        session: String,
        currentTimeMillis: Long = System.currentTimeMillis(),
    ) {
        details.time = currentTimeMillis
        registerMessage(details, session)
        batchBuilder.addGroupsBuilder() += details.rawMessage
    }

    private fun onMessageRequest(
        details: RequestedMessageDetails,
        batchBuilder: DemoGroupBatch,
        session: String,
        currentTimeMillis: Long = System.currentTimeMillis(),
    ) {
        details.time = currentTimeMillis
        registerMessage(details, session)
        batchBuilder.groups.add(DemoMessageGroup(mutableListOf(details.demoRawMessage)))
    }

    private fun send(batchBuilder: MessageGroupBatch.Builder, session: String) {
        val batch = batchBuilder.build()
        if (codecUsePinAttributes) {
            this.messageRouterRawBatch.send(batch, session, QueueAttribute.RAW.value)
        } else {
            this.messageRouterRawBatch.sendAll(batch, QueueAttribute.RAW.value)
        }
    }

    private fun send(batchBuilder: DemoGroupBatch, session: String) {
        if (codecUsePinAttributes) {
            this.messageRouterDemoBatch.send(batchBuilder, session, "demo")
        } else {
            this.messageRouterDemoBatch.sendAll(batchBuilder, "demo")
        }
    }

    private fun registerMessage(message: RequestedMessageDetails, session: String) {
        this.decodeBuffer.add(message, session)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val FROM_CODEC_ATTR = "from_codec"
    }

}