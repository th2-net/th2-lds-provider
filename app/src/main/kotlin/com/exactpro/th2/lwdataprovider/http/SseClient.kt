/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import io.javalin.http.Context
import io.javalin.util.JavalinLogger
import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Copied from [io.javalin.http.sse.SseClient]
 * Because we need to modify the way it handles the event content
 */
class SseClient internal constructor(
    private val ctx: Context,
    private val flushAfter: Int,
) : Closeable {
    init {
        require(flushAfter >= 0) { "flushAfter must be 0 or positive" }
    }

    private val terminated = AtomicBoolean(false)
    private val emitter = Emitter(ctx.res(), flushAfter == 0)
    private var blockingFuture: CompletableFuture<*>? = null
    private var closeCallback = Runnable {}
    private var emitted: Int = 0

    fun ctx(): Context = ctx

    /**
     * @see [io.javalin.http.sse.SseClient.terminated]
     */
    fun terminated() = terminated.get()

    /**
     * @see [io.javalin.http.sse.SseClient.keepAlive]
     */
    fun keepAlive() {
        this.blockingFuture = CompletableFuture<Nothing?>().also { ctx.future { it } }
    }

    /**
     * @see [io.javalin.http.sse.SseClient.onClose]
     */
    fun onClose(closeCallback: Runnable) {
        this.closeCallback = closeCallback
    }

    /**
     * @see [io.javalin.http.sse.SseClient.close]
     */
    override fun close() {
        JavalinLogger.info("Try to close sse client ${ctx.url()}")
        if (terminated.getAndSet(true)) return
        if (flushAfter > 0) {
            emitter.flush()
        }
        closeCallback.run()
        blockingFuture?.complete(null)
        JavalinLogger.info("Sse client ${ctx.url()} has been closed")
    }

    fun sendEvent(event: String, data: ByteArray, id: String? = null) {
        if (terminated.get()) return logTerminated()
        emitter.emit(event, data, id)
        emitted++
        @Suppress("ConvertTwoComparisonsToRangeCheck")
        if (flushAfter > 0 && emitted >= flushAfter) {
            emitter.flush()
            emitted = 0
        }
        checkClosed()
    }

    fun flush() {
        if (terminated.get()) return logTerminated()
        emitter.flush()
        emitted = 0
        checkClosed()
    }

    private fun checkClosed() {
        if (emitter.closed) { // can detect only on write
            this.close()
        }
    }

    private fun logTerminated() = JavalinLogger.warn("Cannot send data, SseClient has been terminated.")

}