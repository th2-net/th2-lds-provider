/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.http.serializers.BookIdDeserializer
import com.exactpro.th2.lwdataprovider.producers.MessageProducer
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53
import com.exactpro.th2.lwdataprovider.producers.MessageProducer53Transport
import com.fasterxml.jackson.databind.module.SimpleModule
import io.github.oshai.kotlinlogging.KotlinLogging
import io.javalin.Javalin
import io.javalin.config.JavalinConfig
import io.javalin.http.BadRequestResponse
import io.javalin.http.ContentType
import io.javalin.http.HttpStatus
import io.javalin.json.JavalinJackson
import io.javalin.micrometer.MicrometerPlugin
import io.javalin.openapi.OpenApiContact
import io.javalin.openapi.OpenApiLicense
import io.javalin.openapi.plugin.OpenApiPlugin
import io.javalin.openapi.plugin.redoc.ReDocPlugin
import io.javalin.openapi.plugin.swagger.SwaggerPlugin
import io.javalin.validation.Validation
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.Tag
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.apache.commons.lang3.exception.ExceptionUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.util.compression.CompressionPool
import org.eclipse.jetty.util.compression.DeflaterPool
import org.eclipse.jetty.util.thread.ThreadPool.SizedThreadPool
import java.time.Instant
import kotlin.math.pow

class HttpServer(private val context: Context) {

    private val jacksonMapper = context.jacksonMapper
    private val configuration = context.configuration

    private var app: Javalin? = null


    fun run() {

        val searchMessagesHandler = this.context.searchMessagesHandler
        val keepAliveHandler = this.context.keepAliveHandler

        val sseResponseBuilder = SseResponseBuilder(
            jacksonMapper,
            if (configuration.listOfMessageAsSingleMessage) {
                if (configuration.useTransportMode) {
                    MessageProducer53Transport.Companion::createMessage
                } else {
                    MessageProducer.Companion::createMessage
                }
            } else {
                if (configuration.useTransportMode) {
                    error("transport mod does not support merging of multiple messages in a single one")
                } else {
                    MessageProducer53.Companion::createMessage
                }
            }
        )
        val handlers: Collection<JavalinHandler> = listOf(
            GetMessagesServlet(
                configuration, context.convExecutor, sseResponseBuilder,
                keepAliveHandler, searchMessagesHandler, context.requestsDataMeasurement
            ),
            GetMessageGroupsServlet(
                configuration, context.convExecutor, sseResponseBuilder,
                keepAliveHandler, searchMessagesHandler, context.requestsDataMeasurement
            ),
            GetMessageById(
                configuration, context.convExecutor,
                sseResponseBuilder, searchMessagesHandler, context.requestsDataMeasurement
            ),
            GetOneEvent(
                sseResponseBuilder,
                this.context.searchEventsHandler,
                context.requestsDataMeasurement,
            ),
            GetEventsServlet(
                configuration, sseResponseBuilder, keepAliveHandler,
                this.context.searchEventsHandler,
                context.convExecutor, context.requestsDataMeasurement,
            ),
            GetBookIDs(context.generalCradleHandler),
            GetSessionAliases(context.searchMessagesHandler),
            GetEventScopes(context.searchEventsHandler),
            GetMessageGroups(context.searchMessagesHandler),
            GetPageInfosServlet(
                configuration, sseResponseBuilder,
                keepAliveHandler, context.generalCradleHandler,
                context.convExecutor, context.requestsDataMeasurement,
            ),
            GetAllPageInfosServlet(
                configuration, sseResponseBuilder,
                keepAliveHandler, context.generalCradleHandler,
                context.convExecutor, context.requestsDataMeasurement,
            ),
            GetSingleMessageByGroupAndId(
                searchMessagesHandler,
                configuration,
                sseResponseBuilder,
                context.convExecutor,
                context.requestsDataMeasurement,
            ),
            DownloadMessagesHandler(
                configuration,
                context.convExecutor,
                sseResponseBuilder,
                context.keepAliveHandler,
                context.searchMessagesHandler,
                context.requestsDataMeasurement,
            ),
            DownloadEventsHandler(
                configuration,
                context.convExecutor,
                sseResponseBuilder,
                context.keepAliveHandler,
                context.searchEventsHandler,
                context.requestsDataMeasurement,
            ),
            TaskDownloadHandler(
                configuration,
                context.convExecutor,
                sseResponseBuilder,
                context.keepAliveHandler,
                context.searchMessagesHandler,
                context.searchEventsHandler,
                context.requestsDataMeasurement,
                context.taskManager,
            ),
        )

        app = Javalin.create { config ->
            config.showJavalinBanner = false
            config.jsonMapper(JavalinJackson(
                jacksonMapper.registerModule(
                    SimpleModule("th2").apply {
                        addDeserializer(BookId::class.java, BookIdDeserializer())
                    }
                )
            ))
            if (logger.isTraceEnabled()) {
                config.bundledPlugins.enableDevLogging()
            } else {
                config.requestLogger.http { ctx, time ->
                    logger.info { "Request ${ctx.method().name} '${ctx.path()}' executed with status ${ctx.status()}: ${time}.ms" }
                }
            }
            config.registerPlugin(MicrometerPlugin { micrometer ->
                micrometer.registry =
                    PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)
                micrometer.tags = listOf(Tag.of("application", context.applicationName))
            })

            val externalContextPath = System.getenv(EXTERNAL_CONTEXT_PATH_ENV)?.takeUnless(String::isBlank)

            setupOpenApi(config)

            setupSwagger(config)

            setupReDoc(config, externalContextPath)

            setupConverters(config)
        }.apply {
            val javalinContext = JavalinContext(configuration.flushSseAfter)
            for (handler in handlers) {
                handler.setup(this, javalinContext)
            }
            setupExceptionHandlers(this)
            jettyServer()?.server()?.let { server ->
                server.insertHandler(createGzipHandler(server, configuration.gzipCompressionLevel))
            }
        }.start(configuration.hostname, configuration.port)

        logger.info { "serving on: http://${configuration.hostname}:${configuration.port}" }
    }

    fun stop() {
        app?.stop()
        logger.info { "http server stopped" }
    }

    private fun setupReDoc(config: JavalinConfig, externalContextPath: String?) {
        config.registerPlugin(ReDocPlugin { userConfig ->
            externalContextPath?.also { path ->
                userConfig.basePath = path
            }
        })
    }

    private fun setupSwagger(config: JavalinConfig) {
        config.registerPlugin(SwaggerPlugin())
    }

    private fun setupOpenApi(config: JavalinConfig) {
        config.registerPlugin(OpenApiPlugin { userConfig ->
            userConfig.withDefinitionConfiguration { _, definition ->
                definition.withInfo { openApiInfo ->
                    val openApiContact = OpenApiContact()
                    openApiContact.name = "Exactpro DEV"
                    openApiContact.email = "dev@exactprosystems.com"

                    val openApiLicense = OpenApiLicense()
                    openApiLicense.name = "Apache 2.0"
                    openApiLicense.identifier = "Apache-2.0"

                    openApiInfo.title = "Light Weight Data Provider"
                    openApiInfo.summary = "API for getting data from Cradle"
                    openApiInfo.description =
                        "Light Weight Data Provider provides you with fast access to data in Cradle"
                    openApiInfo.contact = openApiContact
                    openApiInfo.license = openApiLicense
                    openApiInfo.version = "2.6.0"
                }.withServer { openApiServer ->
                    openApiServer.url = "http://localhost:{port}"
                    openApiServer.addVariable("port", "8080", arrayOf("8080"), "Port of the server")
                }
            }
        })
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        private const val EXTERNAL_CONTEXT_PATH_ENV = "EXTERNAL_CONTEXT_PATH"

        const val TIME_EXAMPLE =
            "Every value that is greater than 1_000_000_000 ^ 2 will be interpreted as nanos. Otherwise, as millis.\n" +
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
        fun setupConverters(config: JavalinConfig) {
            config.validation.register(Instant::class.java) {
                val value = it.toLong()
                convertToInstant(value)
            }
            config.validation.register(ProviderEventId::class.java, ::ProviderEventId)
            config.validation.register(SearchDirection::class.java, SearchDirection::valueOf)
            config.validation.register(BookId::class.java, ::BookId)
            Validation.addValidationExceptionMapper(config)
        }

        @JvmStatic
        fun setupExceptionHandlers(javalin: Javalin) {
            val function: (exception: Exception, ctx: io.javalin.http.Context) -> Unit = { ex, ctx ->
                ctx.contentType(ContentType.APPLICATION_JSON)
                throw BadRequestResponse(ExceptionUtils.getRootCauseMessage(ex))
            }
            javalin.exception(IllegalArgumentException::class.java, function)
            javalin.exception(InvalidRequestException::class.java, function)
            javalin.exception(Exception::class.java) { ex, ctx ->
                ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .json(ExceptionInfo(ex::class.java.canonicalName, ExceptionUtils.getRootCauseMessage(ex)))
            }
        }
    }
}

private fun createGzipHandler(server: Server, gzipCompressionLevel: Int): GzipHandler {
    return GzipHandler().apply {
        // copied from DeflaterPool.ensurePool method
        val capacity = server.getBean(SizedThreadPool::class.java)?.maxThreads ?: CompressionPool.DEFAULT_CAPACITY

        deflaterPool = DeflaterPool(capacity, gzipCompressionLevel, true)
        setExcludedMimeTypes(*excludedMimeTypes.asSequence()
            .filter { it != "text/event-stream" }
            .toList().toTypedArray())
        //FIXME: The sync flush should be used in case of streaming
        isSyncFlush = false
    }
}