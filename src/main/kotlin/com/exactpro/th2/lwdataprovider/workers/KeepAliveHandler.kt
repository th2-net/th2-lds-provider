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

import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class KeepAliveHandler(private val configuration: Configuration) {
    
    private val data: MutableList<RequestContext> = ArrayList();
    private val running = AtomicBoolean(false)
    private val timeout = configuration.keepAliveTimeout
    private var thread: Thread? = null
    
    fun addKeepAliveData(requestContext: RequestContext) {
        data.add(requestContext)
    }

    fun removeKeepAliveData(requestContext: RequestContext) {
        data.remove(requestContext)
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
        
        while (running.get()) {
            
            data.forEach {
                if (System.currentTimeMillis() - it.scannedObjectInfo.timestamp >= timeout)
                    it.keepAliveEvent()
            }

            try {
                Thread.sleep(timeout)
            } catch (e: InterruptedException) {
                running.set(false)
                TODO("add logging")
            }
            
        }
        
        
    }
}