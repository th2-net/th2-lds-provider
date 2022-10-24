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

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import com.exactpro.th2.dataprovider.grpc.TimeRelation as GrpcTimeRelation

class TestSseEventSearchRequest {

    @Nested
    inner class TestConstructorParam(){
        // when startTimestamp and resumeFromId - nulls, should throw exception
        @Test
        fun testEmptyParamsMap(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(mapOf())
            }
        }

        // resumeFromId != null, startTimestamp and endTimestamp - nulls
        @Test
        fun testTimestampNullsResumeIdNotNull(){
            val eventSearchReq = SseEventSearchRequest(mapOf("resumeFromId" to listOf("1")))
            Assertions.assertNull(eventSearchReq.startTimestamp, "start timestamp must be null")
            Assertions.assertNull(eventSearchReq.endTimestamp, "end timestamp must be null")
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val eventSearchReq = SseEventSearchRequest(mapOf("startTimestamp" to listOf("1"),
                "endTimestamp" to listOf("2", "3")))
            Assertions.assertEquals(TimeRelation.AFTER, eventSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! > eventSearchReq.startTimestamp!!){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be after start timestamp: " + eventSearchReq.startTimestamp

            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndAfterStartDirectionBefore(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(mapOf("startTimestamp" to listOf("1"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous")))
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndBeforeStartDirectionBefore(){
            val eventSearchReq = SseEventSearchRequest(mapOf("startTimestamp" to listOf("3"),
                "endTimestamp" to listOf("2", "3"), "searchDirection" to listOf("previous")))
            Assertions.assertEquals(TimeRelation.BEFORE, eventSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! < eventSearchReq.startTimestamp!!){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be before start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(mapOf("startTimestamp" to listOf("3"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("next")))
            }
        }
    }

    @Nested
    inner class TestConstructorGrpc {
        // when startTimestamp and resumeFromId - nulls, should throw exception
        @Test
        fun testEmptyRequest(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException") {
                SseEventSearchRequest(EventSearchRequest.newBuilder().build())
            }
        }

        // resumeFromId != null, startTimestamp and endTimestamp - nulls
        @Test
        fun testTimestampNullsResumeIdNotNull(){
            val grpcRequest = EventSearchRequest.newBuilder().setResumeFromId(EventID.newBuilder().setId("1")).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertNull(eventSearchReq.startTimestamp, "start timestamp must be null")
            Assertions.assertNull(eventSearchReq.endTimestamp, "end timestamp must be null")
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.AFTER, eventSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! > eventSearchReq.startTimestamp!!){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be after start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndAfterStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
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
            val grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.PREVIOUS).build()
            val eventSearchReq = SseEventSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.BEFORE, eventSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(eventSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(eventSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(eventSearchReq.endTimestamp!! < eventSearchReq.startTimestamp!!){
                "end timestamp: " + eventSearchReq.endTimestamp + " must be before start timestamp: " + eventSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.NEXT).build()
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseEventSearchRequest(grpcRequest)
            }
        }
    }
}