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

package com.exactpro.th2.lwdataprovider.workers

import com.exactpro.th2.lwdataprovider.configuration.Configuration
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class TimerWatcher(
    private val decodeBuffer: TimeoutChecker,
    configuration: Configuration
) {
    
    private val timeout: Long = configuration.decodingTimeout
    private val running = AtomicBoolean()
    private var thread: Thread? = null

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    

    fun start() {
        running.set(true)
        thread?.interrupt()
        thread = thread(name="timeout-watcher", start = true, priority = 2) { run() }
    }

    fun stop() {
        if (running.compareAndSet(true, false)) {
            thread?.interrupt()
        } else {
            logger.warn { "Timeout  watcher already stopped" }
        }
    }
    
    private fun run() {

        logger.info { "Timeout watcher started" }
        try {
            while (running.get()) {
                val minTime: Long = try {
                    decodeBuffer.removeOlderThen(timeout)
                } catch (ex: Exception) {
                    logger.error(ex) { "cannot remove old elements from buffer" }
                    System.currentTimeMillis()
                }

                val sleepTime = timeout - (System.currentTimeMillis() - minTime)
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime)
                    } catch (e: InterruptedException) {
                        if (running.compareAndSet(true, false)) {
                            logger.warn(e) { "Someone stopped timeout watcher" }
                            break
                        }
                    }
                }
            }
        } finally {
            logger.info { "Timeout watcher finished" }
        }
    }
}