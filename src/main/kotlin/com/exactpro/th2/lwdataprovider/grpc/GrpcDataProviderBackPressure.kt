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

import com.exactpro.th2.lwdataprovider.CancelableResponseHandler
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.grpc.Status
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class GrpcDataProviderBackPressure(
    configuration: Configuration,
    searchMessagesHandler: SearchMessagesHandler,
    searchEventsHandler: SearchEventsHandler,
    dataMeasurement: DataMeasurement,
    private val scheduler: ScheduledExecutorService,
) : GrpcDataProviderImpl(configuration, searchMessagesHandler, searchEventsHandler, dataMeasurement) {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun <T> processResponse(
        responseObserver: StreamObserver<T>,
        buffer: BlockingQueue<GrpcEvent>,
        handler: CancelableResponseHandler,
        onFinished: () -> Unit,
        converter: (GrpcEvent) -> T?
    ) {
        val servCallObs = responseObserver as ServerCallStreamObserver<T>
        val lock = ReentrantLock()
        var future: Future<*>? = null

        fun cleanBuffer() {
            while (buffer.poll() != null) {
                buffer.clear()
            }
        }
        fun cancel() {
            handler.cancel()
            onClose(handler)
            cleanBuffer()
            onFinished()
        }
        servCallObs.setOnReadyHandler {
            if (!handler.isAlive)
                return@setOnReadyHandler
            lock.withLock {
                future?.cancel(false)
                future = null
            }
            var inProcess = true
            while (servCallObs.isReady && inProcess) {
                if (servCallObs.isCancelled) {
                    logger.warn { "Request is canceled during processing" }
                    handler.cancel()
                    return@setOnReadyHandler
                }
                val event = buffer.take()
                if (event.close) {
                    servCallObs.onCompleted()
                    inProcess = false
                    onFinished()
                    onClose(handler)
                    logger.info { "Executing finished successfully" }
                } else if (event.error != null) {
                    servCallObs.onError(event.error)
                    inProcess = false
                    onFinished()
                    handler.complete()
                    logger.warn(event.error) { "Executing finished with error" }
                } else {
                    converter.invoke(event)?.let {  servCallObs.onNext(it) }
                }
            }
            if (inProcess) {
                lock.withLock {
                    future = scheduler.schedule({
                        runCatching {
                            logger.error {
                                "gRPC was not ready more than ${configuration.grpcBackPressureReadinessTimeoutMls} mls. " +
                                        "In queue ${buffer.size}. Close the connection"
                            }
                            servCallObs.onError(
                                Status.DEADLINE_EXCEEDED
                                    .withDescription("gRPC was not ready longer than configured timeout")
                                    .asRuntimeException()
                            )
                            cancel()
                        }
                    }, configuration.grpcBackPressureReadinessTimeoutMls, TimeUnit.MILLISECONDS)
                }
            }
            if (!servCallObs.isReady) {
                logger.trace { "Suspending processing because the opposite side is not ready to receive more messages. In queue: ${buffer.size}" }
            }
        }

        servCallObs.setOnCancelHandler {
            logger.warn{ "Execution cancelled" }
            lock.withLock {
                future?.cancel(true)
                future = null
            }
            cancel()
        }


    }
}