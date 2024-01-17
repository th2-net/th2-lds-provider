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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.metrics.LIVENESS_MONITOR
import com.exactpro.th2.common.metrics.READINESS_MONITOR
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.configuration.CustomConfigurationClass
import com.exactpro.th2.lwdataprovider.configuration.Mode
import com.exactpro.th2.lwdataprovider.grpc.GrpcServer
import com.exactpro.th2.lwdataprovider.http.HttpServer
import com.exactpro.th2.lwdataprovider.metrics.DecodingMetrics
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

class Main {

    private val configurationFactory: CommonFactory

    private val context: Context

    private val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()

    constructor(args: Array<String>) {

        configureShutdownHook(resources, lock, condition)

        configurationFactory = CommonFactory.createFromArguments(*args)
        resources += configurationFactory

        val configuration =
            Configuration(configurationFactory.getCustomConfiguration(CustomConfigurationClass::class.java))

        logger.info { "Final configuration: $configuration" }

        context = Context(
            configuration,

            cradleManager = configurationFactory.cradleManager.also {
                resources += AutoCloseable { it.close() }
            },
            protoMessageRouter = configurationFactory.messageRouterMessageGroupBatch,
            transportMessageRouter = configurationFactory.transportGroupBatchRouter,
            eventRouter = configurationFactory.eventBatchRouter,
            applicationName = configurationFactory.boxConfiguration?.boxName ?: "lw-data-provider",
        )
    }


    fun run() {
        logger.info { "Starting the box" }

        LIVENESS_MONITOR.enable()

        startServer()

        READINESS_MONITOR.enable()

        awaitShutdown(lock, condition)
    }

    private fun startServer() {
        setupMetrics(context)
        
        context.keepAliveHandler.start()
        context.timeoutHandler.start()

        resources += AutoCloseable {  context.keepAliveHandler.stop() }
        resources += AutoCloseable {  context.timeoutHandler.stop() }

        @Suppress("LiftReturnOrAssignment")
        when (context.configuration.mode) {
            Mode.HTTP ->  {
                val httpServer = HttpServer(context)
                httpServer.run()
                resources += AutoCloseable { httpServer.stop() }
            }
            Mode.GRPC -> {
                val grpcServer = GrpcServer.createGrpc(context, this.configurationFactory.grpcRouter)
                resources += AutoCloseable {
                    grpcServer.stop()
                    grpcServer.blockUntilShutdown()
                }
            }
        }


    }

    private fun setupMetrics(ctx: Context) {
        DecodingMetrics.setMaxDecodeTimeout(ctx.configuration.decodingTimeout.toInt() / 1_000)
        DecodingMetrics.setMaxWaitingQueue(ctx.configuration.maxBufferDecodeQueue)
    }

    private fun configureShutdownHook(resources: Deque<AutoCloseable>, lock: ReentrantLock, condition: Condition) {
        Runtime.getRuntime().addShutdownHook(thread(
            start = false,
            name = "Shutdown hook"
        ) {
            logger.info { "Shutdown start" }
            READINESS_MONITOR.disable()
            lock.withLock { condition.signalAll() }
            resources.descendingIterator().forEachRemaining { resource ->
                try {
                    resource.close()
                } catch (e: Exception) {
                    logger.error(e) { "Cannot close resource ${resource::class}" }
                }
            }
            LIVENESS_MONITOR.disable()
            logger.info { "Shutdown end" }
        })
    }

    @Throws(InterruptedException::class)
    private fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
        lock.withLock {
            logger.info { "Wait shutdown" }
            condition.await()
            logger.info { "App shutdown" }
        }
    }
}


fun main(args: Array<String>) {
    try {
        Main(args).run()
    } catch (ex: Exception) {
        logger.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}
