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

import com.exactpro.th2.lwdataprovider.Context
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import mu.KotlinLogging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet.ServletHandler
import org.eclipse.jetty.servlet.ServletHolder

class HttpServer(private val context: Context) {


    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val jacksonMapper = context.jacksonMapper
    private val configuration = context.configuration

    private var httpServer: Server? = null
    
    
    fun run() {

        val searchMessagesHandler = this.context.searchMessagesHandler
        val keepAliveHandler = this.context.keepAliveHandler
        
        val server = Server()
        httpServer = server
        val connector = ServerConnector(server)
        connector.host = configuration.hostname
        connector.port = configuration.port
        server.connectors = arrayOf(connector)
        val servletHandler = ServletHandler()
        server.handler = servletHandler
        server.insertHandler(GzipHandler())

        val sseResponseBuilder = SseResponseBuilder(jacksonMapper)

        servletHandler.addServletWithMapping(ServletHolder(
            GetMessagesServlet(configuration, sseResponseBuilder, keepAliveHandler,
            searchMessagesHandler, context.dataMeasurement)
        ), "/search/sse/messages")

        servletHandler.addServletWithMapping(ServletHolder(
            GetMessageById(
                sseResponseBuilder, keepAliveHandler, searchMessagesHandler,
                context.dataMeasurement
            )
        ), "/message/*")

        servletHandler.addServletWithMapping(ServletHolder(
            GetOneEvent(sseResponseBuilder, keepAliveHandler, this.context.searchEventsHandler)
        ), "/event/*")
        
        servletHandler.addServletWithMapping(ServletHolder(
            GetTestSpeedServlet()
        ), "/search/sse/test")
        servletHandler.addServletWithMapping(ServletHolder(
            GetTestSpeedServlet2()
        ), "/search/sse/test2")
        servletHandler.addServletWithMapping(ServletHolder(
            GetEventsServlet(configuration, sseResponseBuilder, keepAliveHandler,
            this.context.searchEventsHandler)
        ), "/search/sse/events")

        servletHandler.addServletWithMapping(
            ServletHolder(
                GetSessionAliases(searchMessagesHandler, jacksonMapper),
            ),
            "/messageStreams",
        )

        server.start()

        logger.info { "serving on: http://${configuration.hostname}:${configuration.port}" }
    }

    fun stop() {
        httpServer?.stop()
        logger.info { "http server stopped" }
    }
    
}