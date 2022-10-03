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

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.CustomJsonFormatter
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Mode
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.metrics.CradleSearchMessageMethod
import com.exactpro.th2.lwdataprovider.metrics.LOAD_EVENTS_FROM_CRADLE_COUNTER
import com.exactpro.th2.lwdataprovider.metrics.SEND_EVENTS_COUNTER
import com.exactpro.th2.lwdataprovider.metrics.SEND_MESSAGES_COUNTER
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import io.prometheus.client.Counter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong


class MessageSseRequestContext (
    override val channelMessages: SseResponseHandler,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo(),
    requestedMessages: MutableMap<String, RequestedMessageDetails<SseEvent>> = ConcurrentHashMap(),
    val jsonFormatter: CustomJsonFormatter = CustomJsonFormatter(),
    maxMessagesPerRequest: Int = 0,
    cradleSearchMessageMethod: CradleSearchMessageMethod
) : MessageRequestContext<SseEvent>(
    channelMessages,
    requestParameters,
    counter,
    scannedObjectInfo,
    cradleSearchMessageMethod,
    requestedMessages,
    maxMessagesPerRequest = maxMessagesPerRequest
) {
    override val sendResponseCounter: Counter.Child = SEND_MESSAGES_COUNTER
        .labels(requestId, Mode.HTTP.name, cradleSearchMessageMethod.name)

    override fun createMessageDetails(id: String, time: Long, storedMessage: StoredMessage, responseFormats: List<String>, onResponse: () -> Unit) : RequestedMessageDetails<SseEvent> {
        return SseRequestedMessageDetails(id, time, storedMessage, this, responseFormats, onResponse)
    }

    override fun addStreamInfo() {

    }

}

class SseRequestedMessageDetails(
    id: String,
    time: Long,
    storedMessage: StoredMessage,
    override val context: MessageSseRequestContext,
    responseFormats: List<String>,
    onResponse: () -> Unit,
    parsedMessage: List<Message>? = null,
    rawMessage: RawMessage? = null
) : RequestedMessageDetails<SseEvent>(id, time, storedMessage, context, responseFormats, parsedMessage, rawMessage, onResponse) {

    override fun responseMessageInternal() {
        val msg = MessageProducer53.createMessage(this, context.jsonFormatter)
        val event = context.channelMessages.responseBuilder.build(msg, this.context.counter)
        context.channelMessages.put(event)
    }

}

abstract class EventRequestContext<T> (
    channelMessages: ResponseHandler<T>,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) : RequestContext<T>(channelMessages, requestParameters, counter, scannedObjectInfo) {

    override val loadFromCradleCounter: Counter.Child = LOAD_EVENTS_FROM_CRADLE_COUNTER
        .labels(requestId)

    private var processedEvents: Int = 0
    var eventsLimit: Int = 0

    abstract fun processEvent(event: Event)

    fun addProcessedEvents(count: Int) {
        this.processedEvents += count
    }

    @Suppress("ConvertTwoComparisonsToRangeCheck")
    fun isLimitReached():Boolean {
        return eventsLimit > 0 && processedEvents >= eventsLimit
    }

}

class SseEventRequestContext (
    override val channelMessages: SseResponseHandler,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) : EventRequestContext<SseEvent>(channelMessages, requestParameters, counter, scannedObjectInfo) {

    override val sendResponseCounter: Counter.Child = SEND_EVENTS_COUNTER
        .labels(requestId, Mode.HTTP.name)

    override fun processEvent(event: Event) {
        val sseEvent = channelMessages.responseBuilder.build(event, counter)
        channelMessages.put(sseEvent)
        scannedObjectInfo.update(event.eventId, System.currentTimeMillis(), counter)
    }
}