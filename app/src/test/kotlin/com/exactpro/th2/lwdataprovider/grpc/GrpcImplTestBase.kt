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
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.util.DummyDataMeasurement
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.BindableService
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import java.util.Queue
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


abstract class GrpcImplTestBase {

    protected val executor: Executor = Executors.newSingleThreadExecutor()
    protected val storage = mock<CradleStorage>()
    protected val manager = mock<CradleManager> {
        on { storage } doReturn storage
    }
    protected val searchEventsHandler: SearchEventsHandler = mock {  }
    protected val generalCradleHandler: GeneralCradleHandler = mock {  }
    protected val measurement: DataMeasurement = mock {
        on { start(any()) } doReturn mock { }
    }
    protected val decoder = spy(TestDecoder())
    protected val searchHandler = createSearchMessagesHandler(decoder, false)
    protected val configuration = Configuration(CustomConfigurationClass())

    protected open class TestDecoder(
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

    abstract fun createGrpcDataProvider(): DataProviderGrpc.DataProviderImplBase

    protected class GrpcTestHolder(
        service: BindableService
    ) : AutoCloseable {
        private val serverExecutor = Executors.newFixedThreadPool(1)
        private val clientExecutor = Executors.newFixedThreadPool(1)

        private val inProcessServer: Server = InProcessServerBuilder
            .forName(SERVER_NAME)
            .addService(service)
            .executor(serverExecutor)
            .build()
            .also(Server::start)

        private val inProcessChannel: ManagedChannel = InProcessChannelBuilder
            .forName(SERVER_NAME)
            .executor(clientExecutor)
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