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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.fasterxml.jackson.annotation.JsonRawValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.Instant

data class BaseEventEntity(
    val type: String = "event",
    val id: ProviderEventId,
    val batchId: StoredTestEventId?,
    val isBatched: Boolean,
    val eventName: String,
    val eventType: String,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: ProviderEventId?,
    val successful: Boolean,
    val bookId: String,
    val scope: String,
    val attachedMessageIds: Set<String> = emptySet(),
    @JsonRawValue
    val body: String? = null,
) {
    
    companion object {
        private val mapper = jacksonObjectMapper()
        fun checkAndConvertBody(srcBody: String?) : String {
            return if (srcBody.isNullOrEmpty()) {
                "[]"
            } else if (srcBody.first().let {  it == '[' || it == '{'} && srcBody.last().let { it == ']' || it == '}'}) {
                srcBody
            } else {
                mapper.writeValueAsString(srcBody)
            }
        }
        
    }


    fun convertToEvent(): Event {
        return Event(
            batchId = batchId?.toString(),
            isBatched = batchId != null,
            eventId = id.toString(),
            eventName = eventName,
            eventType = eventType,
            startTimestamp = startTimestamp,
            endTimestamp = endTimestamp,
            parentEventId = parentEventId,
            successful = successful,
            bookId = bookId,
            scope = scope,
            attachedMessageIds = attachedMessageIds,
            body = checkAndConvertBody(body)
        )
    }
}
