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
import io.javalin.openapi.OpenApiInfo
import io.javalin.openapi.OpenApiLicense
import io.javalin.openapi.OpenApiServer
import io.javalin.openapi.OpenApiServerVariable
import io.javalin.openapi.plugin.OpenApiConfiguration
import io.javalin.openapi.plugin.OpenApiPlugin
import io.javalin.openapi.plugin.redoc.ReDocConfiguration
import io.javalin.openapi.plugin.redoc.ReDocPlugin
import io.javalin.openapi.plugin.swagger.SwaggerConfiguration
import io.javalin.openapi.plugin.swagger.SwaggerPlugin
import io.javalin.validation.JavalinValidation
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import java.time.Instant

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
            exception(IllegalArgumentException::class.java) { ex, _ -> throw BadRequestResponse(ExceptionUtils.getRootCauseMessage(ex)) }
            exception(InvalidRequestException::class.java) { ex, _ -> throw BadRequestResponse(ExceptionUtils.getRootCauseMessage(ex)) }
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
        val openApiContact = OpenApiContact()
        openApiContact.name = "Exactpro DEV"
        openApiContact.email = "dev@exactprosystems.com"

        val openApiLicense = OpenApiLicense()
        openApiLicense.name = "Apache 2.0"
        openApiLicense.identifier = "Apache-2.0"

        val openApiInfo = OpenApiInfo()
        openApiInfo.title = "Light Weight Data Provider"
        openApiInfo.summary = "API for getting data from Cradle"
        openApiInfo.description = "Light Weight Data Provider provides you with fast access to data in Cradle"
        openApiInfo.contact = openApiContact
        openApiInfo.license = openApiLicense
        openApiInfo.version = "2.0.0"

        val portServerVariable = OpenApiServerVariable()
        portServerVariable.values = arrayOf("8080")
        portServerVariable.default = "8080"
        portServerVariable.description = "Port of the server"

        val openApiServer = OpenApiServer()
        openApiServer.url = "http://localhost:{port}"
        openApiServer.addVariable("port", portServerVariable)

        val servers = arrayOf(openApiServer)

        val openApiConfiguration = OpenApiConfiguration()
        openApiConfiguration.info = openApiInfo
        openApiConfiguration.servers = servers
        it.plugins.register(OpenApiPlugin(openApiConfiguration))
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        @JvmStatic
        fun setupConverters(javalin: Javalin) {
            JavalinValidation.register(Instant::class.java) { Instant.ofEpochMilli(it.toLong()) }
            JavalinValidation.register(ProviderEventId::class.java, ::ProviderEventId)
            JavalinValidation.register(SearchDirection::class.java, SearchDirection::valueOf)
            JavalinValidation.register(BookId::class.java, ::BookId)
            JavalinValidation.addValidationExceptionMapper(javalin)
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