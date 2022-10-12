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

import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.factory.FactorySettings
import com.exactpro.th2.dataprovider.grpc.AsyncDataProviderService
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchResponse
import io.grpc.Server
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.nio.file.Paths
import java.security.SecureRandom
import java.util.concurrent.TimeUnit

@Disabled("manual test")
class CommonThroughputTest {

    private lateinit var common: AbstractCommonFactory

    private lateinit var service: AbstractServer
    private lateinit var server: Server
    private lateinit var autoClient: DataProviderService
    private lateinit var manualClient: AsyncDataProviderService

    private lateinit var sequence: Sequence<MessageGroupsSearchResponse>
    private lateinit var producerSampler: Sampler

    private val prototype = MessageGroupsSearchResponse.newBuilder().apply {
        collectionBuilder.apply {
            for (i in 0 until BATCH_SIZE) {
                addMessages(MessageGroupResponse.newBuilder().apply {
                    addMessageItem(MessageGroupItem.newBuilder().apply {
                        messageBuilder.apply {
                            addField("raw", rndString())
                        }
                    })
                }.build())
            }
        }
    }.build()

    @BeforeEach
    fun beforeEach() {
        LOGGER.info { "batch size is ${prototype.serializedSize} bytes, ${prototype.collection.messagesCount} messages" }

        producerSampler = Sampler("producer", SAMPLING_FREQUENCY)
        sequence = generateSequence {
            prototype.also {
                producerSampler.inc(BATCH_SIZE)
            }
        }.take(SEQUENCE_SIZE)

        common = CommonFactory(FactorySettings().apply {
            grpc = getPath("com/exactpro/th2/lwdataprovider/grpc/grpc.json")
            routerGRPC = getPath("com/exactpro/th2/lwdataprovider/grpc/grpc_router.json")
        })

        autoClient = common.grpcRouter.getService(DataProviderService::class.java)
        manualClient = common.grpcRouter.getService(AsyncDataProviderService::class.java)
    }

    @Test
    fun `server (ON) vs  client (ON) test`() {
        createManualServer()

        val observer = createManualObserver()
        manualClient.searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance(), observer)
        service.await()
        observer.await()
    }

    @Test
    fun ` server (ON) vs  client (OFF) test`() {
        createManualServer()

        val sampler = Sampler("client (OFF)", SAMPLING_FREQUENCY)
        autoClient.searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance()).forEach {
            sampler.inc(it.collection.messagesCount)
        }
        sampler.complete()
    }

    @Test
    fun ` server (OFF) vs  client (ON) test`() {
        createAutoServer()

        val observer = createManualObserver()
        manualClient.searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance(), observer)
        observer.await()
    }

    @Test
    fun ` server (OFF) vs  client (OFF) test`() {
        createAutoServer()

        val sampler = Sampler("client (OFF)", SAMPLING_FREQUENCY)
        autoClient.searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance()).forEach {
            sampler.inc(it.collection.messagesCount)
        }
        sampler.complete()
    }

    @AfterEach
    fun afterEach() {
        server.shutdown()
        if(!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow()
            check(server.awaitTermination(5, TimeUnit.SECONDS)) {
                "Server can not terminate"
            }
        }

        common.close()
    }

    private fun getPath(resourceName: String) =
        Paths.get(
            requireNotNull(Thread.currentThread().contextClassLoader.getResource(resourceName)) {

            }.toURI()
        )

    private fun createManualObserver() = ClientObserver(
        INITIAL_GRPC_REQUEST,
        PERIODICAL_GRPC_REQUEST
    )

    private fun createServer() {
        server = common.grpcRouter.startServer(service).apply(Server::start)
    }

    private fun createManualServer() {
        service = ManualServer(sequence, SAMPLING_FREQUENCY)
        createServer()
    }

    private fun createAutoServer() {
        service = AutoServer(sequence, SAMPLING_FREQUENCY)
        createServer()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val RANDOM = SecureRandom()

        private const val STRING_LENGTH = 256
        private const val BATCH_SIZE = 4_000

        private const val SEQUENCE_SIZE = 1_000
        private const val INITIAL_GRPC_REQUEST = 10

        private const val PERIODICAL_GRPC_REQUEST = 5

        const val SAMPLING_FREQUENCY = 100L * BATCH_SIZE

        init {
            check(SEQUENCE_SIZE > SAMPLING_FREQUENCY / BATCH_SIZE) {
                "The $SAMPLING_FREQUENCY sampling frequency is less than the $SEQUENCE_SIZE sequence size"
            }
        }

        private fun rndString(): String = RANDOM.ints(32, 126)
            .limit(STRING_LENGTH.toLong() - 1)
            .collect({ StringBuilder() }, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString() + "\u0001"
    }
}