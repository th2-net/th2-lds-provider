package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class TestSseEventSearchRequest {
    @Test
    fun testConstructorParam(){
        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        var params = mutableMapOf("startTimestamp" to listOf("1"),
            "endTimestamp" to listOf("2", "3"))
        var eventSearchReq = SseEventSearchRequest(params)
        Assertions.assertNotNull(eventSearchReq.startTimestamp)
        Assertions.assertNotNull(eventSearchReq.endTimestamp)
        Assertions.assertEquals(TimeRelation.AFTER, eventSearchReq.searchDirection)

        // startTimestamp and resumeFromId - nulls
        params.remove("startTimestamp")
        params.remove("endTimestamp")
        assertThrows<InvalidRequestException> {
            eventSearchReq = SseEventSearchRequest(params)
        }

        // resumeFromId != null - testing checkEndTimestamp validation from here till the end
        params["resumeFromId"] = listOf("1")
        eventSearchReq = SseEventSearchRequest(params)
        Assertions.assertNull(eventSearchReq.startTimestamp)
        Assertions.assertNull(eventSearchReq.endTimestamp)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER
        params["startTimestamp"] = listOf("1")
        params["endTimestamp"] = listOf("2")
        params["searchDirection"] = listOf("next")
        eventSearchReq = SseEventSearchRequest(params)
        Assertions.assertNotNull(eventSearchReq.startTimestamp)
        Assertions.assertNotNull(eventSearchReq.endTimestamp)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        params["searchDirection"] = listOf("previous")
        assertThrows<InvalidRequestException> {
            eventSearchReq = SseEventSearchRequest(params)
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        params["startTimestamp"] = listOf("3")
        eventSearchReq = SseEventSearchRequest(params)

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        params["searchDirection"] = listOf("next")
        assertThrows<InvalidRequestException> {
            eventSearchReq = SseEventSearchRequest(params)
        }

    }

    @Test
    fun testConstructorGrpc(){
        // startTimestamp - 1, endTimestamp - 2, searchDirection - AFTER (default)
        var startTimestamp = Timestamp.newBuilder().setNanos(1).build()
        var endTimestamp = Timestamp.newBuilder().setNanos(2).build()
        var grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).build()
        var messageSearchReq = SseEventSearchRequest(grpcRequest)
        Assertions.assertNotNull(messageSearchReq.startTimestamp)
        Assertions.assertNotNull(messageSearchReq.endTimestamp)
        Assertions.assertEquals(TimeRelation.AFTER, messageSearchReq.searchDirection)

        // startTimestamp - 1, endTimestamp - 2, searchDirection - BEFORE
        grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseEventSearchRequest(grpcRequest)
        }

        // startTimestamp - 3, endTimestamp - 2, searchDirection - BEFORE
        startTimestamp = Timestamp.newBuilder().setNanos(3).build()
        grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS).build()
        messageSearchReq = SseEventSearchRequest(grpcRequest)

        // startTimestamp - 3, endTimestamp - 2, searchDirection - AFTER
        grpcRequest = EventSearchRequest.newBuilder().setStartTimestamp(startTimestamp).setEndTimestamp(endTimestamp).
        setSearchDirection(com.exactpro.th2.dataprovider.grpc.TimeRelation.NEXT).build()
        assertThrows<InvalidRequestException> {
            messageSearchReq = SseEventSearchRequest(grpcRequest)
        }
    }
}