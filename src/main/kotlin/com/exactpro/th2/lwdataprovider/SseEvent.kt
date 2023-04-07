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
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53Demo
import com.exactpro.th2.lwdataprovider.entities.responses.ResponseMessage
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.serialization.json.Json
import java.util.*

/**
 * The data class representing an SSE Event that will be sent to the client.
 */

enum class EventType {
    MESSAGE, EVENT, CLOSE, ERROR, KEEP_ALIVE, MESSAGE_IDS, PAGE_INFO;

    val typeName: String = name.lowercase(Locale.getDefault())

    override fun toString(): String = typeName
}

sealed class SseEvent(
    val event: EventType,
) {
    open val data: String = "empty data"
    open val metadata: String? = null

    object Closed : SseEvent(EventType.CLOSE)

    data class EventData(
        override val data: String,
        override val metadata: String,
    ) : SseEvent(EventType.EVENT)

    data class MessageData(
        override val data: String,
        override val metadata: String,
    ) : SseEvent(EventType.MESSAGE)

    data class KeepAliveData(
        override val data: String,
        override val metadata: String,
    ) : SseEvent(EventType.KEEP_ALIVE)

    data class LastMessageIDsData(
        override val data: String,
    ) : SseEvent(EventType.MESSAGE_IDS)

    data class PageInfoData(
        override val data: String,
        override val metadata: String,
    ) : SseEvent(EventType.PAGE_INFO)
    sealed class ErrorData : SseEvent(EventType.ERROR) {
        data class SimpleError(
            override val data: String
        ) : ErrorData()

        data class TimeoutError(
            override val data: String,
            override val metadata: String,
        ) : ErrorData()
    }

    companion object {

        fun build(jacksonMapper: ObjectMapper, event: Event, counter: Long): SseEvent {
            return EventData(
                jacksonMapper.writeValueAsString(event),
                counter.toString(),
            )
        }

        fun build(jacksonMapper: ObjectMapper, message: ResponseMessage, counter: Long): SseEvent {
            return MessageData(
                when(message) {
                    is ProviderMessage53 -> Json.encodeToString(ProviderMessage53.serializer(), message)
                    is ProviderMessage53Demo -> Json.encodeToString(ProviderMessage53Demo.serializer(), message)
                    else -> jacksonMapper.writeValueAsString(message)
                },
                counter.toString(),
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastScannedObjectInfo: LastScannedObjectInfo,
            counter: Long
        ): SseEvent {
            return KeepAliveData(
                jacksonMapper.writeValueAsString(lastScannedObjectInfo),
                counter.toString(),
            )
        }
        
        fun build(jacksonMapper: ObjectMapper, e: Exception): SseEvent {
            var rootCause: Throwable? = e
            while (rootCause?.cause != null) {
                rootCause = rootCause.cause
            }
            return ErrorData.SimpleError(
                jacksonMapper.writeValueAsString(ExceptionInfo(e.javaClass.name,rootCause?.message ?: e.toString())),
            )
        }

        fun build(
            jacksonMapper: ObjectMapper,
            lastIdInStream: Map<Pair<String, Direction>, StoredMessageId?>
        ): SseEvent {
            return LastMessageIDsData(
                jacksonMapper.writeValueAsString(
                    mapOf(
                        "messageIds" to lastIdInStream.entries.associate { it.key to it.value?.toString() }
                    )
                ),
            )
        }

        fun build(jacksonMapper: ObjectMapper, pageInfo: PageInfo, counter: Long): SseEvent {
            return PageInfoData(
                jacksonMapper.writeValueAsString(pageInfo),
                counter.toString(),
            )
        }
    }
}

data class ExceptionInfo(val exceptionName: String, val exceptionCause: String)