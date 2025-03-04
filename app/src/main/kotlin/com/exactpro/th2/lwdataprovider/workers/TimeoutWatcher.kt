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

package com.exactpro.th2.lwdataprovider.workers

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class TimerWatcher(
    private val decodeBuffer: TimeoutChecker,
    private val timeout: Long,
    private val name: String,
) {
    private val running = AtomicBoolean()
    private var thread: Thread? = null

    init {
        require(timeout > 0) { "negative timeout: $timeout" }
        require(name.isNotBlank()) { "blank name" }
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    

    fun start() {
        running.set(true)
        thread?.interrupt()
        thread = thread(name="timeout-watcher-$name", start = true, priority = 2) { run() }
    }

    fun stop() {
        if (running.compareAndSet(true, false)) {
            thread?.interrupt()
        } else {
            logger.warn { "Timeout watcher $name already stopped" }
        }
    }
    
    private fun run() {

        logger.info { "Timeout watcher $name started" }
        try {
            while (running.get()) {
                val minTime: Long = try {
                    decodeBuffer.removeOlderThen(timeout)
                } catch (ex: Exception) {
                    logger.error(ex) { "cannot remove old elements in time watcher $name" }
                    System.currentTimeMillis()
                }

                val sleepTime = timeout - (System.currentTimeMillis() - minTime)
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime)
                    } catch (e: InterruptedException) {
                        if (running.compareAndSet(true, false)) {
                            logger.warn(e) { "Someone stopped timeout watcher $name" }
                            break
                        }
                    }
                }
            }
        } finally {
            logger.info { "Timeout watcher $name finished" }
        }
    }
}