/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
 */
package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createBatches
import com.exactpro.th2.lwdataprovider.util.validateMessagesOrderGrpc
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.kotlin.argThat
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit

abstract class GRPCBaseTests : GrpcImplTestBase() {

    protected fun stopsPullingDataWhenOutOfRangeExists(offsetNewData: Boolean) {
        val startTimestamp = Instant.now()
        val firstEndTimestamp = startTimestamp.plus(10L, ChronoUnit.MINUTES)
        val endTimestamp = firstEndTimestamp.plus(10L, ChronoUnit.MINUTES)
        val aliasesCount = 5
        val increase = 5L
        val firstBatchMessagesCount = (firstEndTimestamp.epochSecond - startTimestamp.epochSecond) / increase
        val firstMessagesPerAlias = firstBatchMessagesCount / aliasesCount

        val lastBatchMessagesCount = (endTimestamp.epochSecond - firstEndTimestamp.epochSecond) / increase
        val lastMessagesPerAlias = lastBatchMessagesCount / aliasesCount

        val firstBatches = createBatches(
            firstMessagesPerAlias,
            aliasesCount,
            overlapCount = 0,
            increase,
            startTimestamp,
            firstEndTimestamp,
        )
        val lastBatches = createBatches(
            lastMessagesPerAlias,
            aliasesCount,
            overlapCount = 0,
            increase,
            firstEndTimestamp,
            endTimestamp,
            aliasIndexOffset = if (offsetNewData) aliasesCount else 0
        )
        val outsideBatches = createBatches(
            10,
            1,
            0,
            increase,
            endTimestamp.plusNanos(1),
            endTimestamp.plus(5, ChronoUnit.MINUTES),
        )
        val group = "test"
        val firstRequestMessagesCount = firstBatches.sumOf { it.messageCount }
        val secondRequestMessagesCount = lastBatches.sumOf { it.messageCount }
        val messagesCount = firstRequestMessagesCount + secondRequestMessagesCount

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == startTimestamp && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(firstBatches))
        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == firstBatches.maxOf { it.lastTimestamp } && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(lastBatches))
        whenever(storage.getGroupedMessageBatches(argThat {
            limit == 1 && groupName == group
        })).thenReturn(ImmutableListCradleResult(outsideBatches))

        val request = MessageGroupsSearchRequest.newBuilder().apply {
            addMessageGroupBuilder().setName("test")
            addResponseFormats(ResponseFormat.BASE_64.name)
            bookIdBuilder.setName("test")
            this.startTimestamp = startTimestamp.toTimestamp()
            this.endTimestamp = endTimestamp.toTimestamp()
            this.keepOpen = true
        }.build()

        val grpcDataProvider = createGrpcDataProvider()
        GrpcTestHolder(grpcDataProvider).use { (stub) ->
            val responses = stub.searchMessageGroups(request).asSequence().toList()

            assertEquals(messagesCount + 1, responses.size) {
                val missing: List<StoredMessage> =
                    (firstBatches.asSequence() + lastBatches.asSequence()).flatMap { it.messages }.filter { stored ->
                        responses.none {
                            val messageId = it.message.messageId
                            messageId.connectionId.sessionAlias == stored.sessionAlias
                                    && messageId.sequence == stored.sequence
                                    && messageId.direction.toCradleDirection() == stored.direction
                        }
                    }.toList()
                "Missing ${missing.size} message(s): $missing"
            }

            validateMessagesOrderGrpc(responses, messagesCount)
        }
    }
}