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

import com.exactpro.th2.lwdataprovider.CustomJsonFormatter
import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.KeepAliveListener
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import com.exactpro.th2.lwdataprovider.handlers.MessageResponseHandler
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import com.google.gson.Gson
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicLong

class HttpMessagesRequestHandler(
    private val buffer: BlockingQueue<SseEvent>,
    private val builder: SseResponseBuilder,
    dataMeasurement: DataMeasurement,
    maxMessagesPerRequest: Int = 0
) : MessageResponseHandler(dataMeasurement, maxMessagesPerRequest), KeepAliveListener {
    private val jsonFormatter = CustomJsonFormatter()
    private val indexer = DataIndexer()

    private val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()

    override val lastTimestampMillis: Long
        get() = scannedObjectInfo.timestamp

    override fun handleNextInternal(data: RequestedMessageDetails) {
        val msg = MessageProducer53.createMessage(data, jsonFormatter)
        buffer.put(builder.build(msg, indexer.nextIndex()))
    }

    override fun complete() {
        buffer.put(SseEvent(event = EventType.CLOSE))
    }

    override fun writeErrorMessage(text: String) {
        buffer.put(SseEvent(Gson().toJson(Collections.singletonMap("message", text)), EventType.ERROR))
    }

    override fun writeErrorMessage(error: Throwable) {
        writeErrorMessage(ExceptionUtils.getMessage(error))
    }

    override fun update() {
        buffer.put(builder.build(scannedObjectInfo, indexer.nextIndex()))
    }
}

class DataIndexer {
    private val counter: AtomicLong = AtomicLong(0L)
    fun nextIndex(): Long = counter.incrementAndGet()
}

class HttpEventResponseHandler(
    private val buffer: BlockingQueue<SseEvent>,
    private val builder: SseResponseBuilder,
) : AbstractCancelableHandler<Event>(), KeepAliveListener {
    private val indexer = DataIndexer()

    private val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()

    override val lastTimestampMillis: Long
        get() = scannedObjectInfo.timestamp

    override fun complete() {
        buffer.put(SseEvent(event = EventType.CLOSE))
    }

    override fun writeErrorMessage(text: String) {
        buffer.put(SseEvent(Gson().toJson(Collections.singletonMap("message", text)), EventType.ERROR))
    }

    override fun writeErrorMessage(error: Throwable) {
        writeErrorMessage(ExceptionUtils.getMessage(error))
    }

    override fun handleNext(data: Event) {
        val index = indexer.nextIndex()
        buffer.put(builder.build(data, index))
        scannedObjectInfo.update(data.eventId, index)
    }

    override fun update() {
        buffer.put(builder.build(scannedObjectInfo, indexer.nextIndex()))
    }

}