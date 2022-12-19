/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.th2.lwdataprovider.KeepAliveListener
import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.failureReason
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import com.exactpro.th2.lwdataprovider.handlers.MessageResponseHandler
import com.exactpro.th2.lwdataprovider.producers.JsonFormatter
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import com.exactpro.th2.lwdataprovider.producers.ParsedFormats
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.EnumSet
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

class HttpMessagesRequestHandler(
    private val buffer: BlockingQueue<Supplier<SseEvent>>,
    private val builder: SseResponseBuilder,
    dataMeasurement: DataMeasurement,
    maxMessagesPerRequest: Int = 0,
    responseFormats: Set<ResponseFormat> = EnumSet.of(ResponseFormat.BASE_64, ResponseFormat.PROTO_PARSED),
) : MessageResponseHandler(dataMeasurement, maxMessagesPerRequest), KeepAliveListener {
    private val includeRaw: Boolean = responseFormats.isEmpty() || ResponseFormat.BASE_64 in responseFormats
    private val jsonFormatter: JsonFormatter? = (responseFormats - ResponseFormat.BASE_64).run {
        when (size) {
            0 -> null
            1 -> single()
            else -> error("more than one parsed format specified: $this")
        }
    }?.run(ParsedFormats::createFormatter)
    private val indexer = DataIndexer()

    private val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()

    override val lastTimestampMillis: Long
        get() = scannedObjectInfo.timestamp

    override fun handleNextInternal(data: RequestedMessageDetails) {
        if (!isAlive) return
        val counter = indexer.nextIndex()
        buffer.put {
            val requestedMessage: RequestedMessage = data.awaitAndGet()
            if (jsonFormatter != null && requestedMessage.parsedMessage == null) {
                builder.codecTimeoutError(requestedMessage.storedMessage.id, counter)
            } else {
                builder.build(
                    MessageProducer53.createMessage(requestedMessage, jsonFormatter, includeRaw),
                    counter,
                )
            }
        }
    }

    override fun complete() {
        if (!isAlive) return
        buffer.put { SseEvent.Closed }
    }

    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        if (!isAlive) return
        buffer.put { SseEvent.ErrorData.SimpleError(failureReason(batchId, id, text)) }
    }

    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        writeErrorMessage(ExceptionUtils.getMessage(error), id, batchId)
    }

    override fun update() {
        if (!isAlive) return
        val counter = indexer.nextIndex()
        buffer.put { builder.build(scannedObjectInfo, counter) }
    }
}

class DataIndexer {
    private val counter: AtomicLong = AtomicLong(0L)
    fun nextIndex(): Long = counter.incrementAndGet()
}

class HttpGenericResponseHandler<T>(
    private val buffer: BlockingQueue<Supplier<SseEvent>>,
    private val builder: SseResponseBuilder,
    private val getId: (T) -> Any,
    private val createEvent: SseResponseBuilder.(data: T, index: Long) -> SseEvent,
) : AbstractCancelableHandler(), ResponseHandler<T>, KeepAliveListener {
    private val indexer = DataIndexer()

    private val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()

    override val lastTimestampMillis: Long
        get() = scannedObjectInfo.timestamp

    override fun complete() {
        if (!isAlive) return
        buffer.put { SseEvent.Closed }
    }

    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        if (!isAlive) return
        buffer.put { SseEvent.ErrorData.SimpleError(failureReason(batchId, id, text)) }
    }

    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        writeErrorMessage(ExceptionUtils.getMessage(error), id, batchId)
    }

    override fun handleNext(data: T) {
        if (!isAlive) return
        val index = indexer.nextIndex()
        buffer.put { builder.createEvent(data, index) }
        scannedObjectInfo.update(getId(data).toString(), index)
    }

    override fun update() {
        if (!isAlive) return
        val counter = indexer.nextIndex()
        buffer.put { builder.build(scannedObjectInfo, counter) }
    }
}