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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import org.apache.commons.lang3.StringUtils.defaultIfBlank

internal abstract class AbstractParsedStoredMessageHandler(
    private val handler: MessageResponseHandler,
    private val measurement: DataMeasurement,
    private val batchSize: Int,
) : ResponseHandler<StoredMessage> {
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

    override fun handleNext(data: StoredMessage) {
        val detail = RequestedMessageDetails(data) {
            handler.requestReceived()
        }
        details += detail
        handler.handleNext(detail)
        if (details.size >= batchSize) {
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
        }
    }
}

internal class ParsedStoredMessageHandler(
    handler: MessageResponseHandler,
    private val decoder: Decoder,
    measurement: DataMeasurement,
    batchSize: Int,
) : AbstractParsedStoredMessageHandler(
    handler,
    measurement,
    batchSize
) {
    private val batch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()

    override fun sendBatchMessage(details: MutableList<RequestedMessageDetails>) {
        try {
            decoder.sendBatchMessage(batch, details, details.first().storedMessage.sessionAlias)
        } finally {
            batch.clear()
        }
    }
}

internal class DemoParsedStoredMessageHandler(
    handler: MessageResponseHandler,
    private val decoder: Decoder,
    measurement: DataMeasurement,
    batchSize: Int,
) : AbstractParsedStoredMessageHandler(
    handler,
    measurement,
    batchSize
) {
    private val batch: DemoGroupBatch = DemoGroupBatch.newMutable()

    override fun sendBatchMessage(details: MutableList<RequestedMessageDetails>) {
        try {
            val first = details.first()
            val sessionAlias = first.storedMessage.sessionAlias

            batch.book = first.storedMessage.bookId.name
            batch.sessionGroup = defaultIfBlank(first.group, sessionAlias)
            decoder.sendBatchMessage(batch, details, sessionAlias)
        } finally {
            batch.softClean()
        }
    }
}