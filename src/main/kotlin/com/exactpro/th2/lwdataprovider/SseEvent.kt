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

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.entities.responses.ResponseMessage
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.*

/**
 * The data class representing an SSE Event that will be sent to the client.
 */

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS, PAGE_INFO;

    val typeName: String = name.lowercase(Locale.getDefault())

    override fun toString(): String = typeName
}

data class SseEvent(val data: String = "empty data", val event: EventType, val metadata: String? = null) {
    companion object {

        fun build(jacksonMapper: ObjectMapper, event: Event, counter: Long): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(event),
                EventType.EVENT,
                counter.toString()
            )
        }

        fun build(jacksonMapper: ObjectMapper, message: ResponseMessage, counter: Long): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(message),
                EventType.MESSAGE,
                counter.toString()
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastScannedObjectInfo: LastScannedObjectInfo,
            counter: Long
        ): SseEvent {
            return SseEvent(
                data = jacksonMapper.writeValueAsString(lastScannedObjectInfo),
                event = EventType.KEEP_ALIVE,
                metadata = counter.toString()
            )
        }
        
        fun build(jacksonMapper: ObjectMapper, e: Exception): SseEvent {
            var rootCause: Throwable? = e
            while (rootCause?.cause != null) {
                rootCause = rootCause.cause
            }
            return SseEvent(
                jacksonMapper.writeValueAsString(ExceptionInfo(e.javaClass.name,rootCause?.message ?: e.toString())),
                event = EventType.ERROR
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>
        ): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(
                    mapOf(
                        "messageIds" to lastIdInStream.entries.associate { it.key to it.value?.toString() }
                    )
                ),
                event = EventType.MESSAGE_IDS
            )
        }

        fun build(jacksonMapper: ObjectMapper, pageInfo: PageInfo, counter: Long): SseEvent {
            return SseEvent(
                jacksonMapper.writeValueAsString(pageInfo),
                EventType.PAGE_INFO,
                counter.toString()
            )
        }
    }
}

data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)