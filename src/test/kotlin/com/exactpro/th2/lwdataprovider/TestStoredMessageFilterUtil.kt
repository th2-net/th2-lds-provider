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

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.handlers.util.modifyFilterBuilderTimestamps
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestStoredMessageFilterUtil {
    @Test
    fun testDirectionAfter(){
        val request = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("1"),
            "endTimestamp" to listOf("2"), "searchDirection" to listOf("next"), "bookId" to listOf("test"), "stream" to listOf("test")))
        val filter = MessageFilterBuilder().apply {
            bookId(BookId("test"))
            sessionAlias("test")
            direction(Direction.FIRST)
            modifyFilterBuilderTimestamps(request)
        }.build()

        assertEquals(request.startTimestamp, filter.timestampFrom.value){
            "timestamp from: " + filter.timestampFrom.value + " must equal start timestamp: " + request.startTimestamp
        }
        assertEquals(request.endTimestamp, filter.timestampTo.value){
            "timestamp to: " + filter.timestampTo.value + " must equal end timestamp: " + request.endTimestamp
        }
    }

    @Test
    fun testDirectionBefore(){
        val request = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("3"),
            "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous"), "bookId" to listOf("test"), "stream" to listOf("test")))
        val filter = MessageFilterBuilder().apply {
            bookId(BookId("test"))
            sessionAlias("test")
            direction(Direction.FIRST)
            modifyFilterBuilderTimestamps(request)
        }.build()

        assertEquals(request.endTimestamp, filter.timestampFrom.value){
            "timestamp from: " + filter.timestampFrom.value + " must equal end timestamp: " + request.endTimestamp
        }
        assertEquals(request.startTimestamp, filter.timestampTo.value){
            "timestamp to: " + filter.timestampTo.value + " must equal start timestamp: " + request.startTimestamp
        }
    }
}