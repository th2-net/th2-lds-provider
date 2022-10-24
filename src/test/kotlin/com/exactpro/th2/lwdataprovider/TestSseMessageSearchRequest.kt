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
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import com.exactpro.th2.dataprovider.grpc.TimeRelation as GrpcTimeRelation

class TestSseMessageSearchRequest {

    @Nested
    inner class TestConstructorParam {
        // when startTimestamp and resumeFromIdsList - nulls, should throw exception
        @Test
        fun testEmptyParamsMap(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(mapOf())
            }
        }

        // resumeFromIdsList != null, startTimestamp and endTimestamp - nulls
        @Test
        fun testTimestampNullsResumeIdListNotNull(){
            val messageSearchReq = SseMessageSearchRequest(mapOf("messageId" to listOf("name:first:1")))
            Assertions.assertNull(messageSearchReq.startTimestamp, "start timestamp must be null")
            Assertions.assertNull(messageSearchReq.endTimestamp, "end timestamp must be null")
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val messageSearchReq = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("1"),
                "endTimestamp" to listOf("2", "3")))
            Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(messageSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(messageSearchReq.endTimestamp, "end time stamp must be not null")
            Assertions.assertTrue(messageSearchReq.endTimestamp!! > messageSearchReq.startTimestamp!!){
                "end timestamp: " + messageSearchReq.endTimestamp + " must be after start timestamp: " + messageSearchReq.startTimestamp
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
            val messageSearchReq = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("3"),
                "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous")))
            Assertions.assertEquals(TimeRelation.BEFORE, messageSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(messageSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(messageSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(messageSearchReq.endTimestamp!! < messageSearchReq.startTimestamp!!){
                "end timestamp: " + messageSearchReq.endTimestamp + " must be after before timestamp: " + messageSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(mapOf("startTimestamp" to listOf("3"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("next")))
            }
        }
    }

    @Nested
    inner class TestConstructorGrpc {
        // when startTimestamp and resumeFromId - nulls, should throw exception
        @Test
        fun testEmptyRequest(){
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(MessageSearchRequest.newBuilder().build())
            }
        }

        // resumeFromIdsList != null, startTimestamp and endTimestamp - nulls
        @Test
        fun testTimestampNullsResumeIdNotNull(){
            val messageStreamP = MessageStreamPointer.newBuilder()
            val grpcRequest = MessageSearchRequest.newBuilder().addStreamPointer(messageStreamP)
                .build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertNull(messageSearchReq.startTimestamp, "start timestamp must be null")
            Assertions.assertNull(messageSearchReq.endTimestamp, "end timestamp must be null")
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        @Test
        fun testEndAfterStartDirectionDefault(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection, "search direction must be AFTER")
            Assertions.assertNotNull(messageSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(messageSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(messageSearchReq.endTimestamp!! > messageSearchReq.startTimestamp!!){
                "end timestamp: " + messageSearchReq.endTimestamp + " must be after start timestamp: " + messageSearchReq.startTimestamp
            }
        }

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndAfterStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.PREVIOUS).build()
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(grpcRequest)
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        @Test
        fun testEndBeforeStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.PREVIOUS).build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.BEFORE, messageSearchReq.searchDirection, "search direction must be BEFORE")
            Assertions.assertNotNull(messageSearchReq.startTimestamp, "start timestamp must be not null")
            Assertions.assertNotNull(messageSearchReq.endTimestamp, "end timestamp must be not null")
            Assertions.assertTrue(messageSearchReq.endTimestamp!! < messageSearchReq.startTimestamp!!){
                "end timestamp: " + messageSearchReq.endTimestamp + " must be before start timestamp: " + messageSearchReq.startTimestamp
            }
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        @Test
        fun testEndBeforeStartDirectionAfter(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(GrpcTimeRelation.NEXT).build()
            assertThrows<InvalidRequestException>("must throw invalidRequestException"){
                SseMessageSearchRequest(grpcRequest)
            }
        }
    }
}
