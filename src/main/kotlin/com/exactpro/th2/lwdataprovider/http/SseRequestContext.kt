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
import com.exactpro.th2.lwdataprovider.*
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53


class MessageSseRequestContext(
    override val channelMessages: SseResponseHandler,
    val jsonFormatter: CustomJsonFormatter = CustomJsonFormatter(),
    maxMessagesPerRequest: Int = 0
) : MessageRequestContext(channelMessages, maxMessagesPerRequest) {


    override fun createMessageDetails(id: String, storedMessage: StoredMessage, onResponse: () -> Unit) : RequestedMessageDetails {
        return SseRequestedMessageDetails(id, storedMessage, this, onResponse)
    }

    override fun addStreamInfo() {

    }

}

class SseRequestedMessageDetails(
    id: String,
    storedMessage: StoredMessage,
    override val context: MessageSseRequestContext,
    onResponse: () -> Unit
) : RequestedMessageDetails(id, storedMessage, context, onResponse) {

    override fun responseMessageInternal() {
        val msg = MessageProducer53.createMessage(this, context.jsonFormatter)
        val event = context.channelMessages.responseBuilder.build(msg, this.context.counter)
        context.channelMessages.buffer.put(event)
    }

}

abstract class EventRequestContext(
    channelMessages: ResponseHandler
) : RequestContext(channelMessages) {

    private var processedEvents: Int = 0;
    var eventsLimit: Int = 0;

    abstract fun processEvent(event: Event);

    fun addProcessedEvents(count: Int) {
        this.processedEvents += count;
    }

    @Suppress("ConvertTwoComparisonsToRangeCheck")
    fun isLimitReached():Boolean {
        return eventsLimit > 0 && processedEvents >= eventsLimit
    }

}

class SseEventRequestContext(
    override val channelMessages: SseResponseHandler
) : EventRequestContext(channelMessages) {

    override fun processEvent(event: Event) {
        val sseEvent = channelMessages.responseBuilder.build(event, counter)
        channelMessages.buffer.put(sseEvent)
        scannedObjectInfo.update(event.eventId, System.currentTimeMillis(), counter)
    }
}