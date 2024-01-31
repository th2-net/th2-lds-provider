/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import com.fasterxml.jackson.databind.JsonNode
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.javalin.Javalin
import io.javalin.http.Header
import io.javalin.json.JavalinJackson
import io.javalin.testtools.HttpClient
import io.javalin.testtools.JavalinTest
import io.javalin.testtools.TestCase
import io.javalin.testtools.TestConfig
import io.prometheus.client.CollectorRegistry
import mu.KotlinLogging
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.*
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.*
import strikt.api.Assertion
import strikt.api.ExpectationBuilder
import strikt.api.expectCatching
import strikt.assertions.isNotNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractHttpHandlerTest<T : JavalinHandler> {
    protected val storage: CradleStorage = mock { }
    private val manager: CradleManager = mock {
        on { storage } doReturn storage
    }
    private val executor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("test-executor-%d")
            .build()
    )
    private val converterExecutor = Executors.newSingleThreadExecutor(
        ThreadFactoryBuilder()
            .setNameFormat("test-conv-executor-%d")
            .build()
    )
    protected open val configuration = Configuration(
        CustomConfigurationClass(
            decodingTimeout = 400,
        )
    )

    private val semaphore = Semaphore(0)
    private fun receivedRequest(invocation: InvocationOnMock) {
        LOGGER.info { "Received ${invocation.method.name} codec request" }
        semaphore.release()
    }

    private val protoMessageListeners = ConcurrentHashMap.newKeySet<MessageListener<MessageGroupBatch>>()
    private val transportMessageListeners = ConcurrentHashMap.newKeySet<MessageListener<GroupBatch>>()
    protected val protoMessageRouter: MessageRouter<MessageGroupBatch> = mock {
        on { subscribeAll(any(), anyVararg()) } doAnswer {
            val listener = it.getArgument<MessageListener<MessageGroupBatch>>(0)
            protoMessageListeners += listener
            mock {
                on { unsubscribe() } doAnswer {
                    protoMessageListeners -= listener
                }
            }
        }
        on { send(any(), anyVararg()) } doAnswer { receivedRequest(it) }
        on { sendAll(any(), anyVararg()) } doAnswer { receivedRequest(it) }
    }
    protected val transportMessageRouter: MessageRouter<GroupBatch> = mock {
        on { subscribeAll(any(), anyVararg()) } doAnswer {
            val listener = it.getArgument<MessageListener<GroupBatch>>(0)
            transportMessageListeners += listener
            mock {
                on { unsubscribe() } doAnswer {
                    transportMessageListeners -= listener
                }
            }
        }
        on { send(any(), anyVararg()) } doAnswer { receivedRequest(it) }
        on { sendAll(any(), anyVararg()) } doAnswer { receivedRequest(it) }
    }

    private val eventRouter: MessageRouter<EventBatch> = mock { }

    protected val context = run {
        Context(
            configuration,
            registry = CollectorRegistry(),
            cradleManager = manager,
            protoMessageRouter = protoMessageRouter,
            transportMessageRouter = transportMessageRouter,
            eventRouter = eventRouter,
            execExecutor = executor,
            convExecutor = converterExecutor,
            applicationName = "test-lw-data-provider",
        )
    }
    protected open val sseResponseBuilder =
        SseResponseBuilder(context.jacksonMapper, MessageProducer53.Companion::createMessage)

    @BeforeAll
    fun setup() {
        context.start()
    }

    @AfterAll
    fun shutdown() {
        context.stop()
        executor.shutdown()
        converterExecutor.shutdown()
    }

    @BeforeEach
    fun cleanup() {
        semaphore.drainPermits()
        reset(storage, protoMessageRouter, eventRouter)
        configureProtoMessageRouter()
        configureTransportMessageRouter()
    }

    private fun configureProtoMessageRouter() {
        whenever(protoMessageRouter.subscribeAll(any(), anyVararg())) doAnswer {
            val listener = it.getArgument<MessageListener<MessageGroupBatch>>(0)
            protoMessageListeners += listener
            mock {
                on { unsubscribe() } doAnswer {
                    protoMessageListeners -= listener
                }
            }
        }
        whenever(protoMessageRouter.send(any(), anyVararg())) doAnswer { receivedRequest(it) }
        whenever(protoMessageRouter.sendAll(any(), anyVararg())) doAnswer { receivedRequest(it) }
    }

    private fun configureTransportMessageRouter() {
        whenever(transportMessageRouter.subscribeAll(any(), anyVararg())) doAnswer {
            val listener = it.getArgument<MessageListener<GroupBatch>>(0)
            transportMessageListeners += listener
            mock {
                on { unsubscribe() } doAnswer {
                    transportMessageListeners -= listener
                }
            }
        }
        whenever(transportMessageRouter.send(any(), anyVararg())) doAnswer { receivedRequest(it) }
        whenever(transportMessageRouter.sendAll(any(), anyVararg())) doAnswer { receivedRequest(it) }
    }

    protected fun startTest(
        testConfig: TestConfig = TestConfig(
            okHttpClient = OkHttpClient.Builder()
                .retryOnConnectionFailure(false) // otherwise, the client does retry on timeout response
                .build()
        ), testCase: TestCase
    ) {
        JavalinTest.test(
            app = Javalin.create {
                it.jsonMapper(JavalinJackson(MAPPER))
                it.plugins.enableDevLogging()
            }.apply {
                val handler = createHandler()
                handler.setup(this, JavalinContext(flushAfter = 0/*auto flush*/))
            }.also(HttpServer.Companion::setupConverters)
                .also(HttpServer.Companion::setupExceptionHandlers),
            config = testConfig,
            testCase,
        )
    }

    protected fun receiveMessages(vararg messages: Message) {
        val batch = MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                for (msg in messages) {
                    this += msg
                }
            }
        }.build()
        notifyListeners(batch)
    }

    protected fun receiveMessagesGroup(vararg messages: Message) {
        val batch = MessageGroupBatch.newBuilder().apply {
            for (msg in messages) {
                addGroupsBuilder().apply {
                    this += msg
                }
            }
        }.build()
        notifyListeners(batch)
    }

    private fun notifyListeners(batch: MessageGroupBatch?) {
        val metadata = DeliveryMetadata("test", isRedelivered = false)
        LOGGER.info { "Await for codec request" }
        Assertions.assertTrue(semaphore.tryAcquire(500, TimeUnit.MILLISECONDS)) {
            "request for decoding was not received during 500 mls"
        }
        LOGGER.info { "Notify ${protoMessageListeners.size} proto listener(s)" }
        protoMessageListeners.forEach { it.handle(metadata, batch) }
    }

    protected fun receiveTransportMessages(book: String, sessionGroup: String, vararg messages: ParsedMessage) {
        messages.first()
        val batch = GroupBatch(
            book,
            sessionGroup,
            messages.asSequence()
                .map { MessageGroup(mutableListOf(it)) }
                .toMutableList()
        )
        val metadata = DeliveryMetadata("test", isRedelivered = false)
        LOGGER.info { "Await for codec request" }
        Assertions.assertTrue(semaphore.tryAcquire(500, TimeUnit.MILLISECONDS)) {
            "request for decoding was not received during 500 mls"
        }
        LOGGER.info { "Notify ${transportMessageListeners.size} transport listener(s)" }
        transportMessageListeners.forEach { it.handle(metadata, batch) }
    }

    abstract fun createHandler(): T

    protected fun Assertion.Builder<Response>.jsonBody(): Assertion.Builder<JsonNode> =
        get { body }.isNotNull().get { MAPPER.readTree(bytes()) }

    protected fun Response.bodyAsJson(): JsonNode =
        MAPPER.readTree(requireNotNull(body) { "empty body" }.bytes())

    protected fun HttpClient.sse(path: String, requestCfg: Request.Builder.() -> Unit = {}): Response = get(path) {
        requestCfg(it)
        it.header(Header.ACCEPT, "text/event-stream")
    }

    companion object {
        private val MAPPER = Context.createObjectMapper()
        private val LOGGER = KotlinLogging.logger { }

        const val BOOK_NAME =
            "test" //TODO: Move to the CradleTestUtil and use in CradleTestUtil.createCradleStoredMessage and all cases where the method used
        const val PAGE_NAME = "test-page"
        const val SESSION_GROUP = "test-session-group"
        const val SESSION_ALIAS = "test-session-alias"
        const val MESSAGE_TYPE = "test-message-type"

        @JvmStatic
        fun expectEventually(
            timeout: Long,
            delay: Long = 100,
            description: String = "expected condition was not met withing $timeout mls",
            action: suspend () -> Boolean,
        ): Assertion.Builder<Result<Unit>> {
            require(timeout > 0) { "invalid timeout value $timeout" }
            require(delay > 0) { "invalid delay value $delay" }
            return expectCatching {
                val deadline = System.currentTimeMillis() + timeout
                while (System.currentTimeMillis() < deadline) {
                    if (action()) {
                        return@expectCatching
                    }
                    Thread.sleep(delay)
                }
                throw IllegalStateException("condition was not met")
            }.describedAs(description)
        }

        @JvmStatic
        fun expectNever(
            timeout: Long,
            delay: Long = 100,
            description: String = "unexpected condition happened within $timeout mls",
            action: suspend () -> Boolean,
        ): Assertion.Builder<Result<Unit>> {
            require(timeout > 0) { "invalid timeout value $timeout" }
            require(delay > 0) { "invalid delay value $delay" }
            return expectCatching {
                val deadline = System.currentTimeMillis() + timeout
                while (System.currentTimeMillis() < deadline) {
                    if (action()) {
                        throw IllegalStateException("condition is met")
                    }
                    Thread.sleep(delay)
                }
            }.describedAs(description)
        }
    }
}