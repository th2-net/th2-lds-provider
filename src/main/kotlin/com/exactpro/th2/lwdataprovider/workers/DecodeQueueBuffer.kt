/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.metrics.DecodingMetrics
import mu.KotlinLogging
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import kotlin.concurrent.withLock

class DecodeQueueBuffer(
    private val maxDecodeQueueSize: Int = -1
) : RequestsBuffer, AutoCloseable {

    private val lock = ReentrantLock()
    @GuardedBy("lock")
    private val decodeQueue: MutableMap<RequestId, MutableList<RequestedMessageDetails>> = HashMap()

    @GuardedBy("lock")
    private val decodeTimers: MutableMap<RequestId, AutoCloseable> = HashMap()

    @GuardedBy("lock")
    private val decodeCond = lock.newCondition()
    @GuardedBy("lock")
    private var locked: Boolean = false
    
    fun add(details: RequestedMessageDetails, session: String) {
        lock.withLock {
            decodeQueue.computeIfAbsent(details.requestId) { ArrayList(1) }.add(details)
            decodeTimers.computeIfAbsent(details.requestId) { DecodingMetrics.startTimer(session) }
            DecodingMetrics.currentWaiting(decodeQueue.size)
        }
    }

    fun checkAndWait(size: Int) {
        if (maxDecodeQueueSize <= 0) return // unlimited
        check(size in 0..maxDecodeQueueSize) { "size of the single request must be less than the max queue size" }
        var submitted = false
        do {
            LOGGER.trace { "Checking decode queue for available space for $size request(s)" }
            // We need to make sure that there are exactly 'maxDecodeQueueSize' requests or less in the result queue
            lock.withLock {
                val newRequests = decodeQueue.size + size
                if (maxDecodeQueueSize < newRequests) {
                    LOGGER.debug { "Cannot fit $size messages. " +
                            "Expected queue size is more than buffer size ($maxDecodeQueueSize < $newRequests) buf and thread will be locked" }

                    locked = true
                    decodeCond.await()
                } else {
                    submitted = true
                }
            }
        } while (!submitted)
        LOGGER.trace { "Decode request for $size message(s) is submitted" }
    }

    override fun responseProtoReceived(id: RequestId, response: () -> List<Message>) {
        processResponse(id, response, RequestedMessageDetails::responseFinished)
    }

    override fun responseTransportReceived(id: RequestId, response: () -> List<ParsedMessage>) {
        processResponse(id, response, RequestedMessageDetails::responseTransportFinished)
    }

    override fun bulkResponsesProtoReceived(responses: Map<RequestId, () -> List<Message>>) {
        // TODO: maybe we should use something optimized for bulk removal instead of simple map
        responses.forEach(this::responseProtoReceived)
    }

    override fun bulkResponsesTransportReceived(responses: Map<RequestId, () -> List<ParsedMessage>>) {
        // TODO: maybe we should use something optimized for bulk removal instead of simple map
        responses.forEach(this::responseTransportReceived)
    }

    override fun removeOlderThan(timeout: Long): Long {
        return withQueueLockAndRelease {
            val currentTime = System.currentTimeMillis()
            var minTime = currentTime
            val entries = decodeQueue.entries.iterator()
            while (entries.hasNext()) {
                val (id, details) = entries.next()
                if (details.any { currentTime - it.time >= timeout }) {
                    entries.remove()
                    decodeTimers.remove(id)?.close()
                    LOGGER.trace { "Requests for message $id were cancelled due to timeout" }
                    details.forEach { it.timeout() }
                } else {
                    // Possible cause of timeout thread death
                    val oldestReq = details.minOf { it.time }
                    if (oldestReq < minTime) {
                        minTime = oldestReq
                    }
                }
            }
            DecodingMetrics.currentWaiting(decodeQueue.size)
            minTime
        }
    }

    override fun close() {
        lock.withLock {
            LOGGER.info { "Closing ${decodeQueue.size} request(s) without response" }
            decodeQueue.forEach { (id, details) ->
                LOGGER.info { "Canceling request for id $id" }
                details.forEach(RequestedMessageDetails::timeout)
            }
        }
    }

    private inline fun <T> withQueueLockAndRelease(block: () -> T): T = try {
        lock.withLock {
            block()
        }
    } finally {
        checkAndUnlock()
    }

    private fun checkAndUnlock() {
        if (maxDecodeQueueSize <= 0) return
        lock.withLock {
            if (!locked) return
            val requests = decodeQueue.size
            if (requests < maxDecodeQueueSize) {
                decodeCond.signalAll()
                LOGGER.debug { "Buffer is unlocked with size $requests" }
                locked = false
            }
            LOGGER.trace { "Buffers size is $requests" }
        }
    }

    private fun <M> processResponse(
        id: RequestId,
        response: () -> List<M>,
        responseFinished: RequestedMessageDetails.(List<M>) -> Unit
    ) {
        val details = withQueueLockAndRelease {
            decodeTimers.remove(id)?.close()
            decodeQueue.remove(id)?.also {
                DecodingMetrics.currentWaiting(decodeQueue.size)
            } ?: run {
                LOGGER.info { "Received unexpected message $id. There is no request for this message in decode queue" }
                return
            }
        }
        val messages = response()
        LOGGER.trace { "Received response for message $id (${messages.size} message(s))" }
        details.forEach {
            it.responseFinished(messages)
        }
        LOGGER.trace { "Response for message $id submitted" }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private fun RequestedMessageDetails.timeout() {
    protoParsedMessages = null
    transportParsedMessages = null
    responseMessage()
}

private fun RequestedMessageDetails.responseFinished(response: List<Message>) {
    protoParsedMessages = response
    responseMessage()
}

private fun RequestedMessageDetails.responseTransportFinished(response: List<ParsedMessage>) {
    transportParsedMessages = response
    responseMessage()
}