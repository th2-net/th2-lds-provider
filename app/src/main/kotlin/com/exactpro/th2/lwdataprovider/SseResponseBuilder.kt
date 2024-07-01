/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.SseEvent.Companion.DATA_CHARSET
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.entities.responses.ResponseMessage
import com.exactpro.th2.lwdataprovider.producers.JsonFormatter
import com.fasterxml.jackson.databind.ObjectMapper

class SseResponseBuilder(
    private val jacksonMapper: ObjectMapper = ObjectMapper(),
    private val responseFactory: (RequestedMessage, JsonFormatter?, Boolean) -> ResponseMessage,
) {

    fun build(
        message: RequestedMessage,
        formatter: JsonFormatter?,
        includeRaw: Boolean,
        counter: Long,
    ): SseEvent {
        return SseEvent.build(jacksonMapper, responseFactory(message, formatter, includeRaw), counter)
    }
    fun build(message: ResponseMessage, counter: Long): SseEvent {
        return SseEvent.build(jacksonMapper, message, counter)
    }

    fun build(lastScannedObjectInfo: LastScannedObjectInfo, counter: Long): SseEvent {
        return SseEvent.build(jacksonMapper, lastScannedObjectInfo, counter)
    }

    fun build(lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>): SseEvent {
        return SseEvent.build(jacksonMapper, lastIdInStream)
    }

    fun build(event: Event, lastEventId: Long): SseEvent {
        return SseEvent.build(event, lastEventId)
    }

    fun build(pageInfo: PageInfo, lastEventId: Long): SseEvent {
        return SseEvent.build(jacksonMapper, pageInfo, lastEventId)
    }

    fun codecTimeoutError(id: StoredMessageId, lastEventId: Long): SseEvent =
        SseEvent.ErrorData.TimeoutError(
            id.failureReason("Codec response wasn't received during timeout").toByteArray(DATA_CHARSET),
            lastEventId.toString()
        )
}