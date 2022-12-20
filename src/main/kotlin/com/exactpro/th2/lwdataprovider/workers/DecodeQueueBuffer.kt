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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import mu.KotlinLogging
import java.util.ArrayList
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.concurrent.GuardedBy
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

class DecodeQueueBuffer(
    private val maxDecodeQueueSize: Int = -1
) : RequestsBuffer, AutoCloseable {

    private val lock = ReentrantReadWriteLock()
    @GuardedBy("lock")
    private val decodeQueue: MutableMap<String, MutableList<RequestedMessageDetails>> = HashMap()

    private val fullDecodeQueryLock = ReentrantLock()
    private val fullDecodeQueryCond = fullDecodeQueryLock.newCondition()
    @GuardedBy("fullDecodeQueryLock")
    private var locked: Boolean = false
    @GuardedBy("fullDecodeQueryLock")
    private var requests: Int = 0
    
    fun add(details: RequestedMessageDetails) {
        lock.write {
            decodeQueue.computeIfAbsent(details.id) { ArrayList(1) }.add(details)
        }
    }

    fun checkAndWait(size: Int) {
        if (maxDecodeQueueSize <= 0) return // unlimited
        check(size in 0..maxDecodeQueueSize) { "size of the single request must be less than the max queue size" }
        var submitted = false
        do {
            LOGGER.trace { "Checking decode queue for available space for $size request(s)" }
            // We need to make sure that there are exactly 'maxDecodeQueueSize' requests or less in the result queue
            fullDecodeQueryLock.withLock {
                val newRequests = lock.read { decodeQueue.size } + size
                if (maxDecodeQueueSize < newRequests) {
                    LOGGER.debug { "Cannot fit $size messages. " +
                            "Expected queue size is more than buffer size ($maxDecodeQueueSize < $newRequests) buf and thread will be locked" }

                    locked = true
                    fullDecodeQueryCond.await()
                } else {
                    submitted = true
                    requests = newRequests
                }
            }
        } while (!submitted)
    }

    override fun responseReceived(id: String, response: () -> List<Message>) {
        withQueueLockAndRelease {
            processResponse(id, response)
        }
    }

    override fun bulkResponsesReceived(responses: Map<String, () -> List<Message>>) {
        withQueueLockAndRelease {
            // TODO: maybe we should use something optimized for bulk removal instead of simple map
            responses.forEach(this::processResponse)
        }
    }

    override fun removeOlderThan(timeout: Long): Long {
        return withQueueLockAndRelease {
            val currentTime = System.currentTimeMillis()
            var mintime = currentTime
            val entries = decodeQueue.entries.iterator()
            while (entries.hasNext()) {
                val (id, details) = entries.next()
                if (details.any { currentTime - it.time >= timeout }) {
                    entries.remove()
                    LOGGER.trace { "Requests for message $id were cancelled due to timeout" }
                    details.forEach { it.timeout() }
                } else {
                    // Possible cause of timeout thread death
                    val oldestReq = details.minOf { it.time }
                    if (oldestReq < mintime) {
                        mintime = oldestReq
                    }
                }
            }
            mintime
        }
    }

    override fun close() {
        lock.write {
            LOGGER.info { "Closing ${decodeQueue.size} request(s) without response" }
            decodeQueue.forEach { (id, details) ->
                LOGGER.info { "Canceling request for id $id" }
                details.forEach(RequestedMessageDetails::timeout)
            }
        }
    }

    private inline fun <T> withQueueLockAndRelease(block: () -> T): T = try {
        lock.write {
            block()
        }
    } finally {
        // we must not hold the `lock.write` before taking `fullDecodeQueryLock` lock
        checkAndUnlock()
    }

    private fun checkAndUnlock() {
        if (maxDecodeQueueSize <= 0) return
        fullDecodeQueryLock.withLock {
            if (!locked) return
            requests = lock.read { decodeQueue.size }
            if (requests < maxDecodeQueueSize) {
                fullDecodeQueryCond.signalAll()
                LOGGER.debug { "Buffer is unlocked with size $requests" }
                locked = false
            }
            LOGGER.trace { "Buffers size is $requests" }
        }
    }

    private fun processResponse(id: String, response: () -> List<Message>) {
        val details = decodeQueue.remove(id) ?: run {
            LOGGER.info { "Received unexpected message $id. There is no request for this message in decode queue" }
            return
        }
        val messages = response()
        LOGGER.trace { "Received response for message $id (${messages.size} message(s))" }
        details.forEach {
            it.responseFinished(messages)
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private fun RequestedMessageDetails.timeout(): Unit = responseFinished(null)

private fun RequestedMessageDetails.responseFinished(response: List<Message>?) {
    parsedMessage = response
    responseMessage()
}