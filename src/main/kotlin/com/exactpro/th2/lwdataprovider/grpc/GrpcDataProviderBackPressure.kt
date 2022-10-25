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

import com.exactpro.cradle.CradleManager
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.grpc.stub.ServerCallStreamObserver
import mu.KotlinLogging

class GrpcDataProviderBackPressure(
    configuration: Configuration,
    searchMessagesHandler: SearchMessagesHandler,
    messageRouter: MessageRouter<MessageGroupBatch>,
    cradleManager: CradleManager,
    searchEventsHandler: SearchEventsHandler
) : GrpcDataProviderImpl(
    configuration,
    searchMessagesHandler,
    messageRouter,
    cradleManager,
    searchEventsHandler
) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun <T> processResponse(
        responseObserver: ServerCallStreamObserver<T>,
        context: RequestContext<GrpcEvent>,
        onFinished: () -> Unit,
        accumulator: Accumulator<T>,
    ) {
        val grpcResponseHandler = context.channelMessages
        responseObserver.setOnReadyHandler {
            if (grpcResponseHandler.streamClosed)
                return@setOnReadyHandler
            var inProcess = true
            while (responseObserver.isReady && inProcess) {
                context.backPressureMetric.off()
                val event = grpcResponseHandler.take()
                if (event.close) {
                    accumulator.get()?.let { responseObserver.onNext(it) }
                    responseObserver.onCompleted()
                    inProcess = false
                    grpcResponseHandler.closeStream()
                    onFinished()
                    onCloseContext(context)
                    logger.info { "Executing finished successfully" }
                } else if (event.error != null) {
                    responseObserver.onError(event.error)
                    inProcess = false
                    grpcResponseHandler.closeStream()
                    onFinished()
                    onCloseContext(context)
                    logger.warn(event.error) { "Executing finished with error" }
                } else {
                    accumulator.accumulateAndGet(event)?.let {  responseObserver.onNext(it) }
                    context.onMessageSent()
                }
            }
            if (inProcess) {
                context.backPressureMetric.on()
                logger.trace { "Suspending processing because the opposite side is not ready to receive more messages. In queue: ${grpcResponseHandler.size}" }
            }
        }

        responseObserver.setOnCancelHandler {
            logger.warn{ "Execution cancelled" }
            grpcResponseHandler.closeStream()
            onCloseContext(context)
            grpcResponseHandler.clear()
            onFinished()
        }


    }
}