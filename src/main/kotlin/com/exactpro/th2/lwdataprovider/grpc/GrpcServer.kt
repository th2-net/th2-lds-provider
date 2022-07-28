/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.lwdataprovider.Context
import io.grpc.BindableService
import io.grpc.Server
import mu.KotlinLogging
import java.util.concurrent.TimeUnit


class GrpcServer (server: Server) {

    companion object {
        private val logger = KotlinLogging.logger { }

        fun createGrpc(context: Context, grpcRouter: GrpcRouter): GrpcServer {
            val bindableService: BindableService = if (context.configuration.grpcBackPressure) {
                logger.info { "Creating grpc provider with back pressure" }
                GrpcDataProviderBackPressure(context.configuration, context.searchMessagesHandler, context.searchEventsHandler)
            } else {
                logger.info { "Creating grpc provider" }
                GrpcDataProviderImpl(context.configuration, context.searchMessagesHandler, context.searchEventsHandler)
            }
            val server = grpcRouter.startServer(bindableService)
            logger.info { "grpc server started" }
            return GrpcServer(server)
        }
    }

    private val server: Server

    init {
        this.server = server
        this.server.start()
        logger.info {"'${GrpcServer::class.java.simpleName}' started" }
    }

    @Throws(InterruptedException::class)
    fun stop() {
        logger.info { "Stopping grpc server" }
        if (server.shutdown().awaitTermination(1, TimeUnit.SECONDS)) {
            logger.warn {"Server isn't stopped gracefully" }
            server.shutdownNow()
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    @Throws(InterruptedException::class)
    fun blockUntilShutdown() {
        server.awaitTermination()
        logger.info { "Grpc server stopped" }
    }

}