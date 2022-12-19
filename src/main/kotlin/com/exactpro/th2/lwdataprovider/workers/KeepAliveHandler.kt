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

import com.exactpro.th2.lwdataprovider.KeepAliveListener
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import mu.KotlinLogging
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class KeepAliveHandler(configuration: Configuration) {
    
    private val data: MutableList<KeepAliveListener> = ArrayList();
    private val running = AtomicBoolean(false)
    private val timeout = configuration.keepAliveTimeout
    private var thread: Thread? = null

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    @Synchronized
    fun addKeepAliveData(listener: KeepAliveListener): AutoCloseable {
        data.add(listener)
        return AutoCloseable {
            removeKeepAliveData(listener)
        }
    }

    @Synchronized
    private fun removeKeepAliveData(listener: KeepAliveListener) {
        data.remove(listener)
    }
    
    fun start() { 
        thread = thread(name="keep-alive-watcher", start = true) { run() }
    }

    fun stop() {
        running.set(false)
        thread?.interrupt()
    }
    
    private fun run() {

        running.set(true)
        logger.info { "Keep alive handler started" }

        try {
            while (running.get()) {

                data.forEach {
                    if (System.currentTimeMillis() - it.lastTimestampMillis >= timeout)
                        it.update()
                }

                try {
                    Thread.sleep(timeout)
                } catch (e: InterruptedException) {
                    if (running.compareAndSet(true, false)) {
                        logger.warn(e) { "Someone stopped keep alive handler" }
                        break
                    }
                }
            }
        } catch (ex: Exception) {
            logger.error(ex) { "unexpected exception in keep alive thread" }
        } finally {
            logger.info { "Keep alive handler finished" }
        }
    }
}