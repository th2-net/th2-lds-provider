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

import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.lwdataprovider.CancelableResponseHandler
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.handlers.MessageResponseHandler
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createBatches
import io.grpc.Context
import io.grpc.StatusRuntimeException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.assertInstanceOf
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.whenever
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.spy
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.verify
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class GrpcDataProviderBackPressureTest : GRPCBaseTests() {

    val backPressureExecutor = Executors.newSingleThreadScheduledExecutor()
    val deadlineExecutor = Executors.newSingleThreadScheduledExecutor()

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `stops pulling if data out of range exist`(offsetNewData: Boolean) {
        this.stopsPullingDataWhenOutOfRangeExists(offsetNewData)
    }

    @ParameterizedTest
    @CsvSource(value = ["true,true", "true,false", "false,true", "false, false"])
    @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
    fun `stops after RST_STREAM event received`(offsetNewData: Boolean, newDataOnEveryRequest: Boolean) {
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
        val emptyBatch = createBatches(
            10,
            0,
            0,
            increase,
            endTimestamp.plusNanos(1),
            endTimestamp.plus(5, ChronoUnit.MINUTES),
        )
        val group = "test"

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == startTimestamp && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(firstBatches))

        whenever(storage.getGroupedMessageBatches(argThat {
            groupName == group && from.value == firstBatches.maxOf { it.lastTimestamp } && to.value == endTimestamp
        })).thenReturn(ImmutableListCradleResult(lastBatches))

        if(newDataOnEveryRequest) {
            whenever(storage.getGroupedMessageBatches(argThat {
                groupName == group  && to.value == endTimestamp
            })).thenReturn(ImmutableListCradleResult(lastBatches))

            whenever(storage.getGroupedMessageBatches(argThat {
                limit == 1 && groupName == group
            })).thenReturn(ImmutableListCradleResult(emptyBatch))
        } else {
            whenever(storage.getGroupedMessageBatches(argThat {
                groupName == group  && to.value == endTimestamp
            })).thenReturn(ImmutableListCradleResult(emptyBatch))

            whenever(storage.getGroupedMessageBatches(argThat {
                limit == 1 && groupName == group
            })).thenReturn(ImmutableListCradleResult(emptyBatch))
        }

        val request = MessageGroupsSearchRequest.newBuilder().apply {
            addMessageGroupBuilder().setName("test")
            addResponseFormats(ResponseFormat.BASE_64.name)
            bookIdBuilder.setName("test")
            this.startTimestamp = startTimestamp.toTimestamp()
            this.endTimestamp = endTimestamp.toTimestamp()
            this.keepOpen = true
        }.build()

        val searchHandler = spy(searchHandler)
        val requestContextCaptor = argumentCaptor<MessageResponseHandler>()
        val grpcDataProvider = GrpcDataProviderBackPressure(
            configuration,
            searchHandler,
            searchEventsHandler,
            generalCradleHandler,
            measurement,
            backPressureExecutor
        )
        GrpcTestHolder(grpcDataProvider).use { (stub) ->

            assertThrows<StatusRuntimeException> {
                withDeadline(100) {
                    val iterator = stub.searchMessageGroups(request)
                    while (iterator.hasNext()) {
                        iterator.next() // to not trigger backpressure
                    }
                }
            }

            verify(searchHandler).loadMessageGroups(
                any(),
                requestContextCaptor.capture(),
                any()
            )

            val capturedRequestContext = requestContextCaptor.firstValue

            assertInstanceOf<CancelableResponseHandler>(capturedRequestContext)

            Thread.sleep(200)

            Assertions.assertFalse(capturedRequestContext.isAlive)
        }
    }

    private fun withDeadline(
        durationMs: Long,
        code: () -> Unit
    ) {
        Context.current()
            .withCancellation()
            .withDeadlineAfter(durationMs, TimeUnit.MILLISECONDS, deadlineExecutor)
            .use { context ->
                context.call {
                    code()
                }
            }
    }

    override fun createGrpcDataProvider(): DataProviderGrpc.DataProviderImplBase = GrpcDataProviderBackPressure(
        configuration,
        searchHandler,
        searchEventsHandler,
        generalCradleHandler,
        measurement,
        backPressureExecutor
    )
}