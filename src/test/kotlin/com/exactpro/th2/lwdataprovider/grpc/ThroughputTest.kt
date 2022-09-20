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
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.MessageGroupItem
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchResponse
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.ClientCallStreamObserver
import io.grpc.stub.ClientResponseObserver
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.security.SecureRandom
import java.text.DecimalFormat
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Disabled("manual test")
class ThroughputTest {

    private val executor = Executors.newFixedThreadPool(2)
    private lateinit var server: Server
    private lateinit var channel: ManagedChannel
    private lateinit var service: AbstractServer
    private lateinit var sequence: Sequence<MessageGroupsSearchResponse>

    private val prototype = MessageGroupsSearchResponse.newBuilder().apply {
        collectionBuilder.apply {
            for (i in 0..BATCH_SIZE) {
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
        val sampler = Sampler("producer")
        sequence = generateSequence {
            prototype.also {
                sampler.inc(BATCH_SIZE)
            }
        }.take(SEQUENCE_SIZE)
    }

    @Test
    fun `manual server vs manual client test`() {
        createManualServer()

        val observer = ClientObserver(
            INITIAL_GRPC_REQUEST,
            PERIODICAL_GRPC_REQUEST
        )
        DataProviderGrpc.newStub(channel).searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance(), observer)
        observer.await()
    }

    @Test
    fun `manual server vs auto client test`() {
        createManualServer()

        val sampler = Sampler("auto client")
        DataProviderGrpc.newBlockingStub(channel).searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance()).forEach {
            sampler.inc(it.collection.messagesCount)
        }

    }

    @Test
    fun `auto server vs manual client test`() {
        createAutoServer()

        val observer = ClientObserver(
            INITIAL_GRPC_REQUEST,
            PERIODICAL_GRPC_REQUEST
        )
        DataProviderGrpc.newStub(channel).searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance(), observer)
        observer.await()
    }

    @Test
    fun `auto server vs auto client test`() {
        createAutoServer()

        val sampler = Sampler("auto client")
        DataProviderGrpc.newBlockingStub(channel).searchMessageGroups(MessageGroupsSearchRequest.getDefaultInstance()).forEach {
            sampler.inc(it.collection.messagesCount)
        }

    }

    @AfterEach
    fun afterEach() {
        service.stop()
        channel.shutdown()
            .awaitTermination(5, TimeUnit.SECONDS)
        channel.shutdownNow()
        server.shutdown()

        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.SECONDS)
        executor.shutdownNow()
    }

    private fun createChannel() {
        channel = InProcessChannelBuilder.forName(NAME)
            .directExecutor() // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
            .usePlaintext().build()
    }

    private fun createServer() {
        server = InProcessServerBuilder
            .forName(NAME)
            .executor(executor)
            .addService(service)
            .build()
            .start()

        createChannel()
    }

    private fun createManualServer() {
        service = ManualServer(sequence)
        createServer()
    }

    private fun createAutoServer() {
        service = AutoServer(sequence)
        createServer()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val NUMBER_FORMAT = DecimalFormat("###,###,###,###.###")
        private val RANDOM = SecureRandom()
        private const val NAME = "test"

        private const val STRING_LENGTH = 256
        private const val BATCH_SIZE = 4_000
        private const val SEQUENCE_SIZE = 1_000
        private const val INITIAL_GRPC_REQUEST = 10_000
        private const val PERIODICAL_GRPC_REQUEST = 1_000

        private const val SAMPLING_FREQUENCY = 100L * BATCH_SIZE

        init {
            check(SEQUENCE_SIZE >= SAMPLING_FREQUENCY / BATCH_SIZE) {
                "The $SAMPLING_FREQUENCY sampling frequency is less than the $SEQUENCE_SIZE sequence size"
            }
        }

        private fun rndString(): String = String(ByteArray(STRING_LENGTH).apply(RANDOM::nextBytes))

        private class Sampler(private val name: String) {
            private val lock = ReentrantLock()
            private var start = System.nanoTime()
            private var count = 0.0

            fun inc(BATCH_SIZE: Int) = lock.withLock {
                count += BATCH_SIZE
                if (count >= SAMPLING_FREQUENCY) {
                    System.nanoTime().also {
                        LOGGER.info { "$name ${NUMBER_FORMAT.format ((count / (it - start)) * 1_000_000_000)} msg/sec" }
                        start = it
                        count = 0.0
                    }
                }
            }
        }

        private abstract class AbstractServer (
            sequence: Sequence<MessageGroupsSearchResponse>,
        ) : DataProviderGrpc.DataProviderImplBase() {
            private val iterator = sequence.iterator()
            protected abstract val sampler: Sampler

            @Volatile
            protected var inProcess = true

            fun stop() {
                inProcess = false
            }

            protected fun send(responseObserver: StreamObserver<MessageGroupsSearchResponse>) {
                if (iterator.hasNext()) {
                    responseObserver.onNext(
                        iterator.next().also { sampler.inc(it.collection.messagesCount) })
                } else {
                    responseObserver.onCompleted()
                    stop()
                }
            }
        }

        private class AutoServer (
            sequence: Sequence<MessageGroupsSearchResponse>,
        ) : AbstractServer(sequence) {
            override val sampler = Sampler("auto server")

            override fun searchMessageGroups(
                request: MessageGroupsSearchRequest,
                responseObserver: StreamObserver<MessageGroupsSearchResponse>
            ) {
                while (inProcess) {
                    send(responseObserver)
                }
            }
        }

        private class ManualServer (
            sequence: Sequence<MessageGroupsSearchResponse>,
        ) : AbstractServer(sequence) {
            override val sampler = Sampler("manual server")

            override fun searchMessageGroups(
                request: MessageGroupsSearchRequest,
                responseObserver: StreamObserver<MessageGroupsSearchResponse>
            ) {
                (responseObserver as ServerCallStreamObserver<MessageGroupsSearchResponse>).apply {
                    responseObserver.setOnReadyHandler {
                        while (responseObserver.isReady && inProcess) {
                           send(responseObserver)
                        }
                    }
                }
            }
        }

        private class ClientObserver(
            private val initialGrpcRequest: Int,
            private val periodicalGrpcRequest: Int,
        ) : ClientResponseObserver<MessageGroupsSearchRequest, MessageGroupsSearchResponse> {
            private val done = CountDownLatch(1)
            private val sampler = Sampler("manual client")

            @Volatile
            private lateinit var requestStream: ClientCallStreamObserver<MessageGroupsSearchRequest>
            private val counter = AtomicInteger()

            override fun beforeStart(requestStream: ClientCallStreamObserver<MessageGroupsSearchRequest>) {
                LOGGER.debug { "beforeStart has been called" }
                this.requestStream = requestStream
                // Set up manual flow control for the response stream. It feels backwards to configure the response
                // stream's flow control using the request stream's observer, but this is the way it is.
                requestStream.disableAutoRequestWithInitial(initialGrpcRequest)
            }

            override fun onNext(value: MessageGroupsSearchResponse) {
                LOGGER.debug { "onNext has been called $value" }
                if (counter.incrementAndGet() % periodicalGrpcRequest == 0) {
                    requestStream.request(periodicalGrpcRequest)
                }
                sampler.inc(value.collection.messagesCount)
            }

            override fun onError(t: Throwable) {
                LOGGER.error(t) { "onError has been called" }
                done.countDown()
            }

            override fun onCompleted() {
                LOGGER.debug { "onCompleted has been called" }
                done.countDown()
            }

            fun await() {
                done.await()
            }
        }
    }
}