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
import com.exactpro.th2.common.message.message
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class TestSseMessageSearchRequest {

    @Nested
    inner class TestConstructorParam {
        @Test
        // when startTimestamp and resumeFromIdsList - nulls, should throw exception
        fun testEmptyParamsMap(){
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(mapOf())
            }
        }

        @Test
        // resumeFromIdsList != null, startTimestamp and endTimestamp - nulls
        fun testTimestampNullsResumeIdListNotNull(){
            val messageSearchReq = SseMessageSearchRequest(mapOf("messageId" to listOf("name:first:1")))
            Assertions.assertNull(messageSearchReq.startTimestamp)
            Assertions.assertNull(messageSearchReq.endTimestamp)
        }

        @Test
        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        fun testEndAfterStartDirectionDefault(){
            val messageSearchReq = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("1"),
                "endTimestamp" to listOf("2", "3")))
            Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection)
            Assertions.assertNotNull(messageSearchReq.startTimestamp)
            Assertions.assertNotNull(messageSearchReq.endTimestamp)
            Assertions.assertTrue(messageSearchReq.endTimestamp!! > messageSearchReq.startTimestamp!!)
        }

        @Test
        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        fun testEndAfterStartDirectionBefore(){
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(mapOf("startTimestamp" to listOf("1"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous")))
            }
        }

        @Test
        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        fun testEndBeforeStartDirectionBefore(){
            val messageSearchReq = SseMessageSearchRequest(mapOf("startTimestamp" to listOf("3"),
                "endTimestamp" to listOf("2"), "searchDirection" to listOf("previous")))
            Assertions.assertEquals(TimeRelation.BEFORE, messageSearchReq.searchDirection)
            Assertions.assertNotNull(messageSearchReq.startTimestamp)
            Assertions.assertNotNull(messageSearchReq.endTimestamp)
            Assertions.assertTrue(messageSearchReq.endTimestamp!! < messageSearchReq.startTimestamp!!)
        }

        @Test
        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        fun testEndBeforeStartDirectionAfter(){
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(mapOf("startTimestamp" to listOf("3"),
                    "endTimestamp" to listOf("2"), "searchDirection" to listOf("next")))
            }
        }
    }

    @Nested
    inner class TestConstructorGrpc {
        @Test
        // when startTimestamp and resumeFromId - nulls, should throw exception
        fun testEmptyRequest(){
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(MessageSearchRequest.newBuilder().build())
            }
        }

        @Test
        // resumeFromIdsList != null, startTimestamp and endTimestamp - nulls
        fun testTimestampNullsResumeIdNotNull(){
            val messageStreamP = MessageStreamPointer.newBuilder()
            val grpcRequest = MessageSearchRequest.newBuilder().addStreamPointer(messageStreamP)
                .build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertNull(messageSearchReq.startTimestamp)
            Assertions.assertNull(messageSearchReq.endTimestamp)
        }

        @Test
        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        fun testEndAfterStartDirectionDefault(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection)
            Assertions.assertNotNull(messageSearchReq.startTimestamp)
            Assertions.assertNotNull(messageSearchReq.endTimestamp)
            Assertions.assertTrue(messageSearchReq.endTimestamp!! > messageSearchReq.startTimestamp!!)
        }

        @Test
        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        fun testEndAfterStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(1).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(grpcRequest)
            }
        }

        @Test
        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        fun testEndBeforeStartDirectionBefore(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
            val messageSearchReq = SseMessageSearchRequest(grpcRequest)
            Assertions.assertEquals(TimeRelation.BEFORE, messageSearchReq.searchDirection)
            Assertions.assertNotNull(messageSearchReq.startTimestamp)
            Assertions.assertNotNull(messageSearchReq.endTimestamp)
            Assertions.assertTrue(messageSearchReq.endTimestamp!! < messageSearchReq.startTimestamp!!)
        }

        @Test
        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        fun testEndBeforeStartDirectionAfter(){
            val startTimestamp = Timestamp.newBuilder().setNanos(3).build()
            val endTimestamp = Timestamp.newBuilder().setNanos(2).build()
            val grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
                setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.NEXT).build()
            assertThrows<InvalidRequestException> {
                SseMessageSearchRequest(grpcRequest)
            }
        }
    }
}
