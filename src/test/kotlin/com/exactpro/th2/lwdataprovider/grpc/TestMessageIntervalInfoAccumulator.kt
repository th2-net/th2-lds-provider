/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStreamInfoOrBuilder
import com.exactpro.th2.lwdataprovider.GrpcEvent
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith

class TestMessageIntervalInfoAccumulator {

    @Test
    fun test() {
        val accumulator = GrpcDataProviderImpl.MessageIntervalInfoAccumulator {
            asSequence()
                .map(MessageStreamInfoOrBuilder::getNumberOfMessages)
                .sum()
        }

        assertNull(accumulator.accumulateAndGet(GrpcEvent(close = true)))
        assertEquals(0L, accumulator.get())
        assertNull(accumulator.accumulateAndGet(GrpcEvent(error = RuntimeException())))
        assertEquals(0L, accumulator.get())
        assertNull(accumulator.accumulateAndGet(GrpcEvent(event = EventResponse.getDefaultInstance())))
        assertEquals(0L, accumulator.get())

        assertFailsWith<java.lang.RuntimeException> {
            accumulator.accumulateAndGet(GrpcEvent(message = MessageSearchResponse.getDefaultInstance()))
        }

        assertNull(accumulator.accumulateAndGet(GrpcEvent(message = generateMessageResponse("S1", FIRST))))
        assertEquals(1L, accumulator.get())
        assertNull(accumulator.accumulateAndGet(GrpcEvent(message = generateMessageResponse("S1", FIRST))))
        assertEquals(2L, accumulator.get())
        assertNull(accumulator.accumulateAndGet(GrpcEvent(message = generateMessageResponse("S2", FIRST))))
        assertEquals(3L, accumulator.get())
        assertNull(accumulator.accumulateAndGet(GrpcEvent(message = generateMessageResponse("S2", SECOND))))
        assertEquals(4L, accumulator.get())
    }

    private fun generateMessageResponse(sessionAlias: String = "S1", direction: Direction = FIRST): MessageSearchResponse? {

        return MessageSearchResponse.newBuilder().apply {
            messageBuilder.apply {
                messageIdBuilder.apply {
                    connectionIdBuilder.apply { this.sessionAlias = sessionAlias }
                    this.direction = direction
                }
            }
        }.build()
    }
}