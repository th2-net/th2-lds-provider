/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.lwdataprovider.BasicResponseHandler
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails

internal abstract class AbstractParsedStoredMessageHandler(
    private val handler: MessageResponseHandler,
    private val batchSize: Int,
    private val batchSizeBytes: Int,
    private val markerAsGroup: Boolean = false,
) : MarkedResponseHandler<String, StoredMessage> {
    private var currentBatchSize: Int = 0
    private val details: MutableList<RequestedMessageDetails> = arrayListOf()
    override val isAlive: Boolean
        get() = handler.isAlive

    override fun complete() {
        processBatch(details)
    }

    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        handler.writeErrorMessage(text, id, batchId)
    }

    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        handler.writeErrorMessage(error, id, batchId)
    }

    override fun handleNext(marker: String, data: StoredMessage) {
        val detail = RequestedMessageDetails(data, sessionGroup = if (markerAsGroup) marker else null) {
            handler.requestReceived()
        }
        details += detail
        currentBatchSize += data.serializedSize
        handler.handleNext(detail)
        if (details.size >= batchSize || currentBatchSize >= batchSizeBytes) {
            processBatch(details)
        }
    }

    protected abstract fun sendBatchMessage(details: MutableList<RequestedMessageDetails>)

    private fun processBatch(details: MutableList<RequestedMessageDetails>) {
        if (details.isEmpty()) {
            return
        }
        try {
            handler.checkAndWaitForRequestLimit(details.size)
            sendBatchMessage(details)
        } finally {
            details.clear()
            currentBatchSize = 0
        }
    }
}

internal interface MarkedResponseHandler<M, V> : BasicResponseHandler {
    fun handleNext(marker: M, data: V)
}

internal class ProtoParsedStoredMessageHandler(
    handler: MessageResponseHandler,
    private val decoder: Decoder,
    batchSize: Int,
    batchSizeBytes: Int,
    markerAsGroup: Boolean = false,
) : AbstractParsedStoredMessageHandler(
    handler,
    batchSize,
    batchSizeBytes,
    markerAsGroup
) {
    private val batch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

    override fun sendBatchMessage(details: MutableList<RequestedMessageDetails>) {
        try {
            decoder.sendBatchMessage(batch, details, details.first().run { sessionGroup ?: storedMessage.sessionAlias })
        } finally {
            batch.clear()
        }
    }
}

internal class TransportParsedStoredMessageHandler(
    handler: MessageResponseHandler,
    private val decoder: Decoder,
    batchSize: Int,
    batchSizeBytes: Int,
    markerAsGroup: Boolean = false,
) : AbstractParsedStoredMessageHandler(
    handler,
    batchSize,
    batchSizeBytes,
    markerAsGroup
) {
    private var batch: GroupBatch.Builder = GroupBatch.builder()

    override fun sendBatchMessage(details: MutableList<RequestedMessageDetails>) {
        try {
            val first = details.first()
            val sessionAlias: String = first.storedMessage.sessionAlias

            batch.setBook(first.storedMessage.bookId.name)
            batch.setSessionGroup(first.sessionGroup?.ifBlank { sessionAlias } ?: sessionAlias)
            decoder.sendBatchMessage(batch, details, batch.sessionGroup)
        } finally {
            batch = GroupBatch.builder()
        }
    }
}