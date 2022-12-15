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
import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.DataMeasurementImpl
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.db.CradleEventExtractor
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.db.GeneralCradleExtractor
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchPageInfosHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.fasterxml.jackson.databind.JsonNode
import io.javalin.Javalin
import io.javalin.http.Header
import io.javalin.json.JavalinJackson
import io.javalin.testtools.HttpClient
import io.javalin.testtools.JavalinTest
import io.javalin.testtools.TestCase
import io.javalin.testtools.TestConfig
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import strikt.api.Assertion
import strikt.assertions.isNotNull
import java.util.concurrent.Executor

abstract class AbstractHttpHandlerTest<T : JavalinHandler>(

) {
    protected val storage: CradleStorage = mock { }
    protected val manager: CradleManager = mock {
        on { storage } doReturn storage
    }
    private val inPlaceExecutor = Executor { it.run() }

    protected val decoder: Decoder = mock { }
    protected val configuration = Configuration(CustomConfigurationClass(
        decodingTimeout = 1
    ))
    protected val sseResponseBuilder = SseResponseBuilder(MAPPER)
    protected val dataMeasurement: DataMeasurement = DataMeasurementImpl

    protected val keepAliveHandler = KeepAliveHandler(configuration)

    protected val messageHandler = SearchMessagesHandler(
        CradleMessageExtractor(100, manager),
        decoder = decoder,
        threadPool = inPlaceExecutor,
        configuration,
    )
    protected val eventsHandler = SearchEventsHandler(
        CradleEventExtractor(manager),
        inPlaceExecutor,
    )
    protected val generalHandler = GeneralCradleHandler(GeneralCradleExtractor(manager))
    protected val pageInfosHandler = SearchPageInfosHandler(
        generalHandler,
        inPlaceExecutor
    )
    fun startTest(testConfig: TestConfig = TestConfig(
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
    abstract fun createHandler(): T

    protected fun Assertion.Builder<Response>.jsonBody(): Assertion.Builder<JsonNode> = get { body }.isNotNull().get { MAPPER.readTree(bytes()) }

    protected fun HttpClient.sse(path: String, requestCfg: Request.Builder.() -> Unit = {}): Response = get(path) {
        requestCfg(it)
        it.header(Header.ACCEPT, "text/event-stream")
    }

    companion object {
        private val MAPPER = Context.createObjectMapper()
    }
}