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

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.handlers.MessageResponseHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.util.DummyDataMeasurement
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import com.exactpro.th2.lwdataprovider.util.createBatches
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.BindableService
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.TimeUnit

// FIXME: refactor and extended
class GrpcDataProviderImplTest {
    private val executor: Executor = Executor { it.run() }
    private val storage = mock<CradleStorage>()
    private val manager = mock<CradleManager> {
        on { storage } doReturn storage
    }
    private val searchEventsHandler: SearchEventsHandler = mock {  }
    private val generalCradleHandler: GeneralCradleHandler = mock {  }
    private val measurement: DataMeasurement = mock {
        on { start(any()) } doReturn mock { }
    }
    private val decoder = spy(TestDecoder())
    private val searchHandler = createSearchMessagesHandler(decoder, false)
    private val configuration = Configuration(CustomConfigurationClass())

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `stops pulling if data out of range exist`(offsetNewData: Boolean) {
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
//        MessagesGroupRequest(
//            groups = setOf("test"),
//            startTimestamp,
//            endTimestamp,
//            keepOpen = true,
//            BookId("test"),
//            responseFormats = setOf(ResponseFormat.BASE_64),
//        )
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

//            val captor = argumentCaptor<RequestedMessageDetails>()
//            verify(handler, atMost(messagesCount)).handleNext(captor.capture())
//            verify(handler, never()).writeErrorMessage(any<String>(), any(), any())
//            verify(handler, never()).writeErrorMessage(any<Throwable>(), any(), any())
//            val messages: List<RequestedMessageDetails> = captor.allValues
//            assertEquals(messagesCount, messages.size) {
//                val missing: List<StoredMessage> =
//                    (firstBatches.asSequence() + lastBatches.asSequence()).flatMap { it.messages }.filter { stored ->
//                        messages.none {
//                            val raw = it.storedMessage
//                            raw.sessionAlias == stored.sessionAlias && raw.sequence == stored.sequence && raw.direction == stored.direction
//                        }
//                    }.toList()
//                "Missing ${missing.size} message(s): $missing"
//            }

//            validateMessagesOrder(messages, messagesCount)
        }
    }

    private open class TestDecoder(
        capacity: Int = 10
    ) : Decoder {
        val protoQueue: Queue<RequestedMessageDetails> = ArrayBlockingQueue(capacity)
        val transportQueue: Queue<RequestedMessageDetails> = ArrayBlockingQueue(capacity)
        override fun sendBatchMessage(
            batchBuilder: MessageGroupBatch.Builder,
            requests: Collection<RequestedMessageDetails>,
            session: String
        ) {
            protoQueue.addAll(requests)
        }

        override fun sendBatchMessage(
            batchBuilder: GroupBatch.Builder,
            requests: Collection<RequestedMessageDetails>,
            session: String
        ) {
            transportQueue.addAll(requests)
        }

        //FIXME: implement for transport
    }

    private fun createSearchMessagesHandler(
        decoder: Decoder,
        useTransportMode: Boolean
    ) = SearchMessagesHandler(
        CradleMessageExtractor(manager, DummyDataMeasurement, false),
        decoder,
        executor,
        Configuration(
            CustomConfigurationClass(
                bufferPerQuery = 4,
                useTransportMode = useTransportMode,
                batchSizeBytes = 300,
            )
        )
    )

    private fun createGrpcDataProvider() = GrpcDataProviderImpl(
        configuration,
        searchHandler,
        searchEventsHandler,
        generalCradleHandler,
        measurement
    )

    private open class MessageResponseHandlerTestImpl(
        measurement: DataMeasurement,
        maxQueue: Int = 0,
    ) : MessageResponseHandler(measurement, maxQueue) {
        override fun handleNextInternal(data: RequestedMessageDetails) {
        }

        override fun complete() {
        }

        override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        }

        override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        }
    }

    private class GrpcTestHolder(
        service: BindableService
    ) : AutoCloseable {
        private val inProcessServer: Server = InProcessServerBuilder
            .forName(SERVER_NAME)
            .addService(service)
            .directExecutor()
            .build()
            .also(Server::start)

        private val inProcessChannel: ManagedChannel = InProcessChannelBuilder
            .forName(SERVER_NAME)
            .directExecutor()
            .build()

        val stub: DataProviderGrpc.DataProviderBlockingStub = DataProviderGrpc.newBlockingStub(inProcessChannel)

        operator fun component1(): DataProviderGrpc.DataProviderBlockingStub = stub

        override fun close() {
            LOGGER.info { "Shutdown process channel" }
            inProcessChannel.shutdown()
            if (!inProcessChannel.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.warn { "Process channel couldn't stop during 1 min" }
                inProcessChannel.shutdownNow()
                LOGGER.warn { "Process channel shutdown now, is terminated: ${inProcessChannel.isTerminated}" }
            }
            LOGGER.info { "Shutdown process server" }
            inProcessServer.shutdown()
            if (!inProcessServer.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.warn { "Process server couldn't stop during 1 min" }
                inProcessServer.shutdownNow()
                LOGGER.warn { "Process server shutdown now, is terminated: ${inProcessChannel.isTerminated}" }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private const val SERVER_NAME = "server"
    }
}