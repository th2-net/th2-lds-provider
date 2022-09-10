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

import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import io.prometheus.client.Gauge
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class DecodeQueueBuffer(private val maxDecodeQueueSize: Int = -1) {

    companion object {
        private val logger = KotlinLogging.logger { }

        private val DECODE_QUEUE_GAUGE = Gauge.build()
            .name("th2_ldp_decode_queue_number")
            .help("Actual number of raw message in decode queue")
            .register()
    }

    private val decodeQueue = ConcurrentHashMap<String, MutableList<RequestedMessageDetails>>()
    private val lock = ReentrantLock()
    
    private val fullDecodeQueryLock = ReentrantLock()
    private val fullDecodeQueryCond = fullDecodeQueryLock.newCondition()
    private var locked: Boolean = false
    
    fun add (details: RequestedMessageDetails): Boolean {
        decodeQueue.computeIfAbsent(details.id) { ArrayList(1) }.add(details)
        return true
    }

    fun removeById (id: String): List<RequestedMessageDetails>? {
        lock.withLock { return decodeQueue.remove(id) }
    }
    
    fun entrySet(): MutableSet<MutableMap.MutableEntry<String, MutableList<RequestedMessageDetails>>> {
        return decodeQueue.entries
    }
    
    @Suppress("ConvertTwoComparisonsToRangeCheck")
    fun checkAndWait() {
        val buf = getSize()
        if (maxDecodeQueueSize > 0 && buf > maxDecodeQueueSize) {
            logger.debug { "Messages in queue is more than buffer size buf and thread will be locked" }
            fullDecodeQueryLock.withLock {
                locked = true
                fullDecodeQueryCond.await()
            }
        }
    }

    fun checkAndUnlock() {
        if (locked && maxDecodeQueueSize > 0 && getSize() < maxDecodeQueueSize) {
            fullDecodeQueryLock.withLock {
                fullDecodeQueryCond.signalAll()
                locked = false
            }
            logger.debug { "Awaiting buffer space is unlocked" }
        }
    }

    private fun getSize(): Int {
        return decodeQueue.size.also {
            DECODE_QUEUE_GAUGE.set(it.toDouble())
        }
    }

    
}