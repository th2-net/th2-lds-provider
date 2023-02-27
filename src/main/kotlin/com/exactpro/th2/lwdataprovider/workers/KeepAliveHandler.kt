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
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.thread
import kotlin.concurrent.write

class KeepAliveHandler(configuration: Configuration) {
    private val lock = ReentrantReadWriteLock()
    private val data: MutableList<KeepAliveListener> = ArrayList();
    private val running = AtomicBoolean(false)
    private val timeout = configuration.keepAliveTimeout
    private var thread: Thread? = null

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    fun addKeepAliveData(listener: KeepAliveListener): AutoCloseable = lock.write {
        data.add(listener)
        AutoCloseable {
            removeKeepAliveData(listener)
        }
    }

    private fun removeKeepAliveData(listener: KeepAliveListener): Unit = lock.write {
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

                lock.read {
                    data.forEach {
                        if (System.currentTimeMillis() - it.lastTimestampMillis >= timeout) {
                            it.update()
                        }
                    }
                }

                try {
                    Thread.sleep(timeout)
                } catch (e: InterruptedException) {
                    if (running.get()) {
                        running.set(false)
                        logger.warn(e) { "Someone stopped keep alive handler" }
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