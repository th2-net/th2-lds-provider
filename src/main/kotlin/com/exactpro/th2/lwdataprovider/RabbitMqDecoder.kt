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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.lwdataprovider.workers.DecodeQueueBuffer
import com.exactpro.th2.lwdataprovider.workers.ProtoCodecMessageListener
import com.exactpro.th2.lwdataprovider.workers.TimeoutChecker
import com.exactpro.th2.lwdataprovider.workers.TransportCodecMessageListener
import mu.KotlinLogging

class RabbitMqDecoder(
    private val protoGroupBatchRouter: MessageRouter<MessageGroupBatch>,
    private val transportGroupBatchRouter: MessageRouter<GroupBatch>,
    maxDecodeQueue: Int,
    private val codecUsePinAttributes: Boolean,
) : TimeoutChecker, Decoder, AutoCloseable {

    private val decodeBuffer = DecodeQueueBuffer(maxDecodeQueue)
    private val protoMonitor = protoGroupBatchRouter.subscribeAll(
        ProtoCodecMessageListener(decodeBuffer),
        QueueAttribute.PARSED.value,
        FROM_CODEC_ATTR
    )
    private val transportMonitor =
        transportGroupBatchRouter.subscribeAll(TransportCodecMessageListener(decodeBuffer), FROM_CODEC_ATTR)

    override fun sendBatchMessage(
        batchBuilder: MessageGroupBatch.Builder,
        requests: Collection<RequestedMessageDetails>,
        session: String
    ) {
        checkAndWaitFreeBuffer(requests.size)
        LOGGER.trace { "Sending proto batch with messages to codec. IDs: ${requests.joinToString { it.requestId.toString() }}" }
        val currentTimeMillis = System.currentTimeMillis()
        onMessageRequest(requests, batchBuilder, session, currentTimeMillis)
        send(batchBuilder, session)
    }

    override fun sendBatchMessage(
        batchBuilder: GroupBatch.Builder,
        requests: Collection<RequestedMessageDetails>,
        session: String
    ) {
        checkAndWaitFreeBuffer(requests.size)
        LOGGER.trace { "Sending transport group batch with messages to codec. IDs: ${requests.joinToString { it.requestId.toString() }}" }
        val currentTimeMillis = System.currentTimeMillis()
        onMessageRequest(requests, batchBuilder, session, currentTimeMillis)
        send(batchBuilder, session)
    }

    override fun removeOlderThen(timeout: Long): Long {
        return decodeBuffer.removeOlderThan(timeout)
    }

    override fun close() {
        runCatching { protoMonitor.unsubscribe() }
            .onFailure { LOGGER.error(it) { "Cannot unsubscribe from proto queue" } }
        runCatching { transportMonitor.unsubscribe() }
            .onFailure { LOGGER.error(it) { "Cannot unsubscribe from transport queue" } }
        decodeBuffer.close()
    }

    private fun checkAndWaitFreeBuffer(size: Int) {
        LOGGER.trace { "Checking if the decoding queue has free $size slot(s)" }
        decodeBuffer.checkAndWait(size)
    }

    private fun onMessageRequest(
        details: Collection<RequestedMessageDetails>,
        batchBuilder: MessageGroupBatch.Builder,
        session: String,
        currentTimeMillis: Long = System.currentTimeMillis(),
    ) {
        registerMessages(details, session)
        details.forEach {
            it.time = currentTimeMillis
            batchBuilder.addGroupsBuilder() += it.protoRawMessage.value
        }
    }

    private fun onMessageRequest(
        details: Collection<RequestedMessageDetails>,
        batchBuilder: GroupBatch.Builder,
        session: String,
        currentTimeMillis: Long = System.currentTimeMillis(),
    ) {
        registerMessages(details, session)
        details.forEach {
            it.time = currentTimeMillis
            batchBuilder.addGroup(MessageGroup(mutableListOf(it.transportRawMessage.value)))
        }
    }

    private fun send(batchBuilder: MessageGroupBatch.Builder, session: String) {
        val batch = batchBuilder.build()
        if (codecUsePinAttributes) {
            this.protoGroupBatchRouter.send(batch, session, QueueAttribute.RAW.value)
        } else {
            this.protoGroupBatchRouter.sendAll(batch, QueueAttribute.RAW.value)
        }
    }

    private fun send(batchBuilder: GroupBatch.Builder, session: String) {
        if (codecUsePinAttributes) {
            this.transportGroupBatchRouter.send(batchBuilder.build(), session)
        } else {
            this.transportGroupBatchRouter.sendAll(batchBuilder.build())
        }
    }

    private fun registerMessages(messages: Collection<RequestedMessageDetails>, session: String) {
        this.decodeBuffer.addAll(messages, session)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val FROM_CODEC_ATTR = "from_codec"
    }

}