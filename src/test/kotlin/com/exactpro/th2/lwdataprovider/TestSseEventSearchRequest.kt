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

import com.exactpro.th2.dataprovider.lw.grpc.BookId
import com.exactpro.th2.dataprovider.lw.grpc.EventScope
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation as GrpcTimeRelation

class TestSseEventSearchRequest {

    @Nested
    inner class TestConstructorParam {
        private fun params(vararg pairs: Pair<String, List<String>>): Map<String, List<String>> {
            return mapOf(
                "bookId" to listOf("test"),
                "scope" to listOf("test-scope"),
            ) + mapOf(*pairs)
        }
        // when startTimestamp and resumeFromId - nulls, should throw exception
        @Test
        fun testEmptyParamsMap(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(mapOf())
            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val eventSearchReq = SseEventSearchRequest(params("startTimestamp" to listOf("1"),
                "endTimestamp" to listOf("2", "3")))
            Assertions.assertEquals(SearchDirection.next, eventSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! > eventSearchReq.startTimestamp){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be after start timestamp: " + eventSearchReq.startTimestamp

            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndAfterStartDirectionBefore(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(params("startTimestamp" to listOf("1"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous")))
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndBeforeStartDirectionBefore(){
            val eventSearchReq = SseEventSearchRequest(params("startTimestamp" to listOf("3"),
                "endTimestamp" to listOf("2", "3"), "searchDirection" to listOf("previous")))
            Assertions.assertEquals(SearchDirection.previous, eventSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! < eventSearchReq.startTimestamp){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be before start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(params("startTimestamp" to listOf("3"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("next")))
            }
        }

        @Test
        fun testLimitNotSetEndTimestampNotSet(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(params("startTimestamp" to listOf("1")))
            }
        }

        @Test
        fun testLimitSetEndTimestampSet(){
            val eventSearchReq = SseEventSearchRequest(params("startTimestamp" to listOf("1"),
                "endTimestamp" to listOf("2"), "resultCountLimit" to listOf("5")))
            Assertions.assertEquals(Instant.ofEpochMilli(2), eventSearchReq.endTimestamp)
        }
    }

    @Nested
    inner class TestConstructorGrpc {
        // when startTimestamp and resumeFromId - nulls, should throw exception
        @Test
        fun testEmptyRequest(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException") {
                SseEventSearchRequest(createBuilder().build())
            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = createBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertEquals(SearchDirection.next, eventSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! > eventSearchReq.startTimestamp){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be after start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndAfterStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = createBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.PREVIOUS).build()
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(grpcRequest)
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndBeforeStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = createBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.PREVIOUS).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertEquals(SearchDirection.previous, eventSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! < eventSearchReq.startTimestamp){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be before start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = createBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.NEXT).build()
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(grpcRequest)
            }
        }

        @Test
        fun testLimitNotSetEndTimestampNotSet(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
                SseEventSearchRequest(createBuilder().setStartTimestamp(startTimestamp).build())
            }
        }

        @Test
        fun testLimitSetEndTimestampSet(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val resultCountLimit = Int32Value.newBuilder().setValue(5).build()
            val grpcRequest = createBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                    setResultCountLimit(resultCountLimit).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertEquals(Instant.ofEpochSecond(0, 2), eventSearchReq.endTimestamp)
        }

        private fun createBuilder(): EventSearchRequest.Builder = EventSearchRequest.newBuilder()
            .setBookId(BookId.newBuilder().setName("test"))
            .setScope(EventScope.newBuilder().setName("test-scope"))
    }
}