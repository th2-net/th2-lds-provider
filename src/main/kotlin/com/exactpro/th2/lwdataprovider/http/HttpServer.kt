/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import io.javalin.Javalin
import io.javalin.config.JavalinConfig
import io.javalin.http.BadRequestResponse
import io.javalin.json.JavalinJackson
import io.javalin.micrometer.MicrometerPlugin
import io.javalin.openapi.OpenApiContact
import io.javalin.openapi.OpenApiLicense
import io.javalin.openapi.plugin.OpenApiPlugin
import io.javalin.openapi.plugin.OpenApiPluginConfiguration
import io.javalin.openapi.plugin.redoc.ReDocConfiguration
import io.javalin.openapi.plugin.redoc.ReDocPlugin
import io.javalin.openapi.plugin.swagger.SwaggerConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerPlugin
import io.javalin.validation.JavalinValidation
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import java.time.Instant
import kotlin.math.pow

class HttpServer(private val context: Context) {

    private val jacksonMapper = context.jacksonMapper
    private val configuration = context.configuration

    private var app: Javalin? = null


    fun run() {

        val searchMessagesHandler = this.context.searchMessagesHandler
        val keepAliveHandler = this.context.keepAliveHandler

        val sseResponseBuilder = SseResponseBuilder(jacksonMapper)
        val handlers: Collection<JavalinHandler> = listOf(
            GetMessagesServlet(configuration, sseResponseBuilder, keepAliveHandler,
                searchMessagesHandler, context.dataMeasurement),
            GetMessageGroupsServlet(configuration, sseResponseBuilder, keepAliveHandler,
                searchMessagesHandler, context.dataMeasurement),
            GetMessageById(
                configuration,
                sseResponseBuilder, searchMessagesHandler, context.dataMeasurement
            ),
            GetOneEvent(configuration, sseResponseBuilder, this.context.searchEventsHandler),
            GetEventsServlet(configuration, sseResponseBuilder, keepAliveHandler,
                this.context.searchEventsHandler),
            GetBookIDs(context.generalCradleHandler),
            GetSessionAliases(context.searchMessagesHandler),
            GetEventScopes(context.searchEventsHandler),
            GetMessageGroups(context.searchMessagesHandler),
            GetPageInfosServlet(configuration, sseResponseBuilder,
                keepAliveHandler, context.generalCradleHandler),
            GetAllPageInfosServlet(configuration, sseResponseBuilder,
                keepAliveHandler, context.generalCradleHandler),
        )

        app = Javalin.create {
            it.showJavalinBanner = false
            it.jsonMapper(JavalinJackson(jacksonMapper))
//            it.plugins.enableDevLogging()
            it.plugins.register(MicrometerPlugin.create {})

            setupOpenApi(it)

            setupSwagger(it)

            setupReDoc(it)
        }.apply {
            setupConverters(this)
            for (handler in handlers) {
                handler.setup(this)
            }
            setupExceptionHandlers(this)
            jettyServer()?.server()?.insertHandler(createGzipHandler())
        }.start(configuration.hostname, configuration.port)

        logger.info { "serving on: http://${configuration.hostname}:${configuration.port}" }
    }

    fun stop() {
        app?.stop()
        logger.info { "http server stopped" }
    }

    private fun setupReDoc(it: JavalinConfig) {
        val reDocConfiguration = ReDocConfiguration()
        it.plugins.register(ReDocPlugin(reDocConfiguration))
    }

    private fun setupSwagger(it: JavalinConfig) {
        val swaggerConfiguration = SwaggerConfiguration()
        it.plugins.register(SwaggerPlugin(swaggerConfiguration))
    }

    private fun setupOpenApi(it: JavalinConfig) {

        val openApiConfiguration = OpenApiPluginConfiguration()
            .withDefinitionConfiguration { _, definition ->
                definition.withOpenApiInfo { openApiInfo ->
                    val openApiContact = OpenApiContact()
                    openApiContact.name = "Exactpro DEV"
                    openApiContact.email = "dev@exactprosystems.com"

                    val openApiLicense = OpenApiLicense()
                    openApiLicense.name = "Apache 2.0"
                    openApiLicense.identifier = "Apache-2.0"

                    openApiInfo.title = "Light Weight Data Provider"
                    openApiInfo.summary = "API for getting data from Cradle"
                    openApiInfo.description = "Light Weight Data Provider provides you with fast access to data in Cradle"
                    openApiInfo.contact = openApiContact
                    openApiInfo.license = openApiLicense
                    openApiInfo.version = "2.0.0"
                }.withServer { openApiServer ->
                    openApiServer.url = "http://localhost:{port}"
                    openApiServer.addVariable("port", "8080", arrayOf("8080"), "Port of the server")
                }
            }
        it.plugins.register(OpenApiPlugin(openApiConfiguration))
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        const val TIME_EXAMPLE = "Every value that is greater than 1_000_000_000 ^ 2 will be interpreted as nanos. Otherwise, as millis.\n" +
                "Millis: 1676023329533, Nanos: 1676023329533590976"

        internal const val NANOS_IN_SECOND = 1_000_000_000L
        /**
         * If we call current time millis it will look like this: 1_676_023_329_533
         * If we call current time nanos it will look like this:  1_676_023_329_533_590_976
         *
         * We can estimate the if values is greater than 1_000_000_000 ^ 2 it is definitely nanos
         */
        private val NANO_SEPARATOR = (1_000_000_000.0).pow(2).toLong()

        @JvmStatic
        internal fun convertToInstant(value: Long): Instant {
            return when {
                value < NANO_SEPARATOR -> Instant.ofEpochMilli(value)
                else -> Instant.ofEpochSecond(
                    value / NANOS_IN_SECOND,
                    value % NANOS_IN_SECOND,
                )
            }
        }

        @JvmStatic
        fun setupConverters(javalin: Javalin) {
            JavalinValidation.register(Instant::class.java) {
                val value = it.toLong()
                convertToInstant(value)
            }
            JavalinValidation.register(ProviderEventId::class.java, ::ProviderEventId)
            JavalinValidation.register(SearchDirection::class.java, SearchDirection::valueOf)
            JavalinValidation.register(BookId::class.java, ::BookId)
            JavalinValidation.addValidationExceptionMapper(javalin)
        }

        @JvmStatic
        fun setupExceptionHandlers(javalin: Javalin) {
            javalin.exception(IllegalArgumentException::class.java) { ex, _ -> throw BadRequestResponse(ExceptionUtils.getRootCauseMessage(ex)) }
            javalin.exception(InvalidRequestException::class.java) { ex, _ -> throw BadRequestResponse(ExceptionUtils.getRootCauseMessage(ex)) }
        }
    }
}

private fun createGzipHandler(): GzipHandler {
    return GzipHandler().apply {
        setExcludedMimeTypes(*excludedMimeTypes.asSequence()
            .filter { it != "text/event-stream" }
            .toList().toTypedArray())
        isSyncFlush = true
    }
}