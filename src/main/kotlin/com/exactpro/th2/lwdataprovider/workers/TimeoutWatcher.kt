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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class TimerWatcher (private val decodeBuffer: DecodeQueueBuffer,
                    private val configuration: Configuration
) {
    
    private val timeout: Long = configuration.decodingTimeout
    private val running = AtomicBoolean(false)
    private var thread: Thread? = null
    

    fun start() {
        thread = thread(name="timeout-watcher", start = true) { run() }
    }

    fun stop() {
        running.set(false)
        thread?.interrupt()
    }
    
    private fun run() {
        
        running.set(true)
        while (running.get()) {
            val currentTime = System.currentTimeMillis()
            var mintime = currentTime

            for (entry in decodeBuffer.entrySet()) {
                var timeoutReached = false
                val iterator = entry.value.iterator()
                while (iterator.hasNext() && !timeoutReached) {
                    val requestedMessageDetails = iterator.next()
                    if (currentTime - requestedMessageDetails.time >= timeout) {
                        //duplicated messages should not be asked again. so timeout is mutual. asked only first (oldest) message
                        val list = decodeBuffer.removeById(entry.key)
                        list?.forEach {
                            it.parsedMessage = null
                            it.responseMessage53()
                            it.notifyMessage()
                        }
                        if (list != null && list.isNotEmpty()) {
                            decodeBuffer.checkAndUnlock()
                        }
                        timeoutReached = true
                    } else if (requestedMessageDetails.time < mintime){
                        mintime = requestedMessageDetails.time
                    }
                }
            }

            val sleepTime = timeout - (System.currentTimeMillis() - mintime)
            if (sleepTime > 0)
                Thread.sleep(sleepTime)
        }
       
    }    
}