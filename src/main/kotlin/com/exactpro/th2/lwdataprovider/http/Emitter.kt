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

import jakarta.servlet.ServletResponse
import jakarta.servlet.WriteListener
import mu.KotlinLogging
import org.eclipse.jetty.server.HttpOutput
import java.io.IOException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

const val NEW_LINE = "\n"

/**
 * Copied from [io.javalin.http.sse.Emitter]
 * because we need to customize the flushing and data writing
 */
class Emitter(
    private val response: ServletResponse,
    private val flushAfter: Int,
) {
    init {
        require(flushAfter >= 0) { "flushAfter must be 0 or positive" }
    }

    private val lock = ReentrantLock()
    private val builder = StringBuilder()
    private val outputStream = (response.outputStream as HttpOutput).apply {
        setWriteListener(object : WriteListener {
            override fun onWritePossible() {
                K_LOGGER.debug { "Write possible" } // FIXME: add any context
            }

            override fun onError(t: Throwable) {
                K_LOGGER.error(t) { t.message }
            }

        })
    }

    var closed = false
        private set

    fun emit(event: String, data: String, id: String?): Unit = lock.withLock {
        try {
            if (id != null) {
                builder.append("id: ").append(id).append(NEW_LINE)
//                write("id: $id$NEW_LINE")
            }
            builder.append("event: ").append(event).append(NEW_LINE)
//            write("event: $event$NEW_LINE")

            builder.append("data: ").append(data).append(NEW_LINE)
//            write("data: $data$NEW_LINE")

            builder.append(NEW_LINE)
//            write(NEW_LINE)

            if (flushAfter == 0 || builder.length > flushAfter) {
                write(builder.toString())
                builder.clear()
            }

        } catch (ignored: IOException) {
            closed = true
        }
    }

    fun flush(): Unit = lock.withLock {
        if (builder.isNotEmpty()) {
            write(builder.toString())
            builder.clear()
        }

        waitReady()
        response.flushBuffer()
    }

    private fun write(value: String) {
        waitReady()
        outputStream.write(value.toByteArray())
    }

    private fun waitReady() {
        while (!outputStream.isReady) {
            Thread.yield()
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {  }
    }
}