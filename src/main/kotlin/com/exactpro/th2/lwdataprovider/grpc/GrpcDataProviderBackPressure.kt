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

import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.grpc.stub.ServerCallStreamObserver
import mu.KotlinLogging

class GrpcDataProviderBackPressure(configuration: Configuration, searchMessagesHandler: SearchMessagesHandler,
    searchEventsHandler: SearchEventsHandler
) : GrpcDataProviderImpl(configuration, searchMessagesHandler, searchEventsHandler) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun <T> processResponse(
        responseObserver: ServerCallStreamObserver<T>,
        grpcResponseHandler: GrpcResponseHandler,
        context: RequestContext,
        onFinished: () -> Unit,
        converter: (GrpcEvent) -> T?
    ) {
        responseObserver.setOnReadyHandler {
            if (grpcResponseHandler.streamClosed)
                return@setOnReadyHandler
            val buffer = grpcResponseHandler.buffer
            var inProcess = true
            while (responseObserver.isReady && inProcess) {
                context.backPressureMetric.off()
                val event = buffer.take()
                if (event.close) {
                    responseObserver.onCompleted()
                    inProcess = false
                    grpcResponseHandler.streamClosed = true
                    onFinished()
                    onCloseContext(context)
                    logger.info { "Executing finished successfully" }
                } else if (event.error != null) {
                    responseObserver.onError(event.error)
                    inProcess = false
                    grpcResponseHandler.streamClosed = true
                    onFinished()
                    onCloseContext(context)
                    logger.warn(event.error) { "Executing finished with error" }
                } else {
                    converter.invoke(event)?.let {  responseObserver.onNext(it) }
                    context.onMessageSent()
                }
            }
            if (!responseObserver.isReady) {
                context.backPressureMetric.on()
                logger.trace { "Suspending processing because the opposite side is not ready to receive more messages. In queue: ${buffer.size}" }
            }
        }

        responseObserver.setOnCancelHandler {
            logger.warn{ "Execution cancelled" }
            grpcResponseHandler.streamClosed = true
            onCloseContext(context)
            val buffer = grpcResponseHandler.buffer
            while (buffer.poll() != null);
            onFinished()
        }


    }
}