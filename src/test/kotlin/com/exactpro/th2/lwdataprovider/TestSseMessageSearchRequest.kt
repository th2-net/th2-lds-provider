package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class TestSseMessageSearchRequest {
    @Test
    fun testConstructorParam(){
        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        var params = mutableMapOf("startTimestamp" to listOf("1"),
            "endTimestamp" to listOf("2", "3"))
        var messageSearchReq = SseMessageSearchRequest(params)
        Assertions.assertNotNull(messageSearchReq.startTimestamp)
        Assertions.assertNotNull(messageSearchReq.endTimestamp)
        Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection)

        // startTimestamp and resumeFromIdsList - nulls
        params.remove("startTimestamp")
        params.remove("endTimestamp")
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseMessageSearchRequest(params)
        }

        // resumeFromIdsList != null - testing checkEndTimestamp validation from here till the end
        params["messageId"] = listOf("name:first:1")
        messageSearchReq = SseMessageSearchRequest(params)
        Assertions.assertNull(messageSearchReq.startTimestamp)
        Assertions.assertNull(messageSearchReq.endTimestamp)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER
        params["startTimestamp"] = listOf("1")
        params["endTimestamp"] = listOf("2")
        params["searchDirection"] = listOf("next")
        messageSearchReq = SseMessageSearchRequest(params)
        Assertions.assertNotNull(messageSearchReq.startTimestamp)
        Assertions.assertNotNull(messageSearchReq.endTimestamp)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        params["searchDirection"] = listOf("previous")
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseMessageSearchRequest(params)
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        params["startTimestamp"] = listOf("3")
        messageSearchReq = SseMessageSearchRequest(params)

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        params["searchDirection"] = listOf("next")
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseMessageSearchRequest(params)
        }
    }

    @Test
    fun testConstructorGrpc(){
        // resumeFromIdsList != null (default)
        var grpcRequest = MessageSearchRequest.newBuilder().build()
        var messageSearchReq = SseMessageSearchRequest(grpcRequest)
        Assertions.assertNull(messageSearchReq.startTimestamp)
        Assertions.assertNull(messageSearchReq.endTimestamp)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        var startTimestamp = Timestamp.newBuilder().setNanos(1).build()
        var endTimestamp = Timestamp.newBuilder().setNanos(2).build()
        grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
        messageSearchReq = SseMessageSearchRequest(grpcRequest)
        Assertions.assertNotNull(messageSearchReq.startTimestamp)
        Assertions.assertNotNull(messageSearchReq.endTimestamp)
        Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseMessageSearchRequest(grpcRequest)
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        startTimestamp = Timestamp.newBuilder().setNanos(3).build()
        grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
        messageSearchReq = SseMessageSearchRequest(grpcRequest)

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        grpcRequest = MessageSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.NEXT).build()
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseMessageSearchRequest(grpcRequest)
        }
    }
}
