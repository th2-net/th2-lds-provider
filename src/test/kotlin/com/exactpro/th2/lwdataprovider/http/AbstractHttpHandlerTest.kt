/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.fasterxml.jackson.databind.JsonNode
import io.javalin.Javalin
import io.javalin.http.Header
import io.javalin.json.JavalinJackson
import io.javalin.testtools.HttpClient
import io.javalin.testtools.JavalinTest
import io.javalin.testtools.TestCase
import io.javalin.testtools.TestConfig
import mu.KotlinLogging
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.mockito.invocation.InvocationOnMock
import org.mockito.kotlin.any
import org.mockito.kotlin.anyVararg
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.reset
import strikt.api.Assertion
import strikt.assertions.isNotNull
import java.util.concurrent.Executor
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractHttpHandlerTest<T : JavalinHandler> {
    protected val storage: CradleStorage = mock { }
    private val manager: CradleManager = mock {
        on { storage } doReturn storage
    }
    private val inPlaceExecutor = Executor { it.run() }
    protected val configuration = Configuration(CustomConfigurationClass(
        decodingTimeout = 100,
    ))

    private val semaphore = Semaphore(0)
    private fun receivedRequest(invocation: InvocationOnMock) {
        LOGGER.info { "Received ${invocation.method.name} codec request" }
        semaphore.release()
    }

    private val messageListeners = arrayListOf<MessageListener<MessageGroupBatch>>()
    private val messageRouter: MessageRouter<MessageGroupBatch> = mock {
        on { subscribeAll(any(), anyVararg()) } doAnswer {
            val listener = it.getArgument<MessageListener<MessageGroupBatch>>(0)
            messageListeners += listener
            mock {
                on { unsubscribe() } doAnswer {
                    messageListeners -= listener
                }
            }
        }
        on { send(any(), anyVararg()) } doAnswer { receivedRequest(it) }
        on { sendAll(any(), anyVararg()) } doAnswer { receivedRequest(it) }
    }

    private val eventRouter: MessageRouter<EventBatch> = mock { }

    protected val context = Context(
        configuration,
        cradleManager = manager,
        messageRouter = messageRouter,
        eventRouter = eventRouter,
        pool = inPlaceExecutor,
    )
    protected val sseResponseBuilder = SseResponseBuilder(context.jacksonMapper)

    @BeforeAll
    fun setup() {
        context.keepAliveHandler.start()
        context.timeoutHandler.start()
    }

    @AfterAll
    fun shutdown() {
        context.keepAliveHandler.stop()
        context.timeoutHandler.stop()
    }

    @BeforeEach
    fun cleanup() {
        reset(storage)
    }

    protected fun startTest(testConfig: TestConfig = TestConfig(
         okHttpClient = OkHttpClient.Builder()
             .retryOnConnectionFailure(false) // otherwise, the client does retry on timeout response
             .build()
    ), testCase: TestCase) {
        JavalinTest.test(
            app = Javalin.create {
                it.jsonMapper(JavalinJackson(MAPPER))
                it.plugins.enableDevLogging()
            }.apply(createHandler()::setup).also(HttpServer.Companion::setupConverters),
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
        LOGGER.info { "Notify ${messageListeners.size} listener(s)" }
        val metadata = DeliveryMetadata("test", isRedelivered = false)
        Assertions.assertTrue(semaphore.tryAcquire(500, TimeUnit.MILLISECONDS)) {
            "request for decoding was not received during 500 mls"
        }
        messageListeners.forEach { it.handle(metadata, batch) }
    }
    abstract fun createHandler(): T

    protected fun Assertion.Builder<Response>.jsonBody(): Assertion.Builder<JsonNode> = get { body }.isNotNull().get { MAPPER.readTree(bytes()) }

    protected fun HttpClient.sse(path: String, requestCfg: Request.Builder.() -> Unit = {}): Response = get(path) {
        requestCfg(it)
        it.header(Header.ACCEPT, "text/event-stream")
    }

    companion object {
        private val MAPPER = Context.createObjectMapper()
        private val LOGGER = KotlinLogging.logger { }
    }
}