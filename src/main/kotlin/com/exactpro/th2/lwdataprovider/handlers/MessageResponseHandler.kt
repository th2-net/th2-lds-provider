/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.GuardedBy
import kotlin.concurrent.withLock

abstract class MessageResponseHandler(
    private val dataMeasurement: DataMeasurement,
    private val maxMessagesPerRequest: Int = 0,
) : AbstractCancelableHandler<RequestedMessageDetails>() {
    private val lock: ReentrantLock = ReentrantLock()
    private val condition: Condition = lock.newCondition()
    protected val streamInfo: ProviderStreamInfo = ProviderStreamInfo()

    @GuardedBy("lock")
    private var messagesInProcess: Int = 0

    @Volatile
    var allMessagesRequested: Boolean = false
        private set

    val isDataProcessed: Boolean
        get() = lock.withLock { messagesInProcess == 0 }

    fun checkAndWaitForRequestLimit(msgBufferCount: Int) {
        var submitted = false
        dataMeasurement.start("await_queue").use {
            do {
                lock.withLock {
                    val expectedSize = messagesInProcess + msgBufferCount
                    @Suppress("ConvertTwoComparisonsToRangeCheck")
                    if (maxMessagesPerRequest > 0 && maxMessagesPerRequest < expectedSize) {
                        condition.await()
                    } else {
                        messagesInProcess = expectedSize
                        submitted = true
                    }
                }
            } while (!submitted)
        }
    }

    fun registerSession(alias: String, direction: Direction) {
        streamInfo.registerSession(alias, direction)
    }

    fun dataLoaded() {
        allMessagesRequested = true
        if (isDataProcessed) {
            complete()
        }
    }

    override fun handleNext(data: RequestedMessageDetails) {
        onMessageReceived()
        streamInfo.registerMessage(data.storedMessage.id)
        try {
            handleNextInternal(data)
        } finally {
            if (allMessagesRequested && isDataProcessed) {
                complete()
            }
        }
    }

    protected abstract fun handleNextInternal(data: RequestedMessageDetails)

    private fun onMessageReceived() {
        lock.withLock {
            if (messagesInProcess == 0) return // if nothing in process just return
            val curCount = messagesInProcess
            messagesInProcess -= 1
            if (maxMessagesPerRequest <= 0) return // if not limit just return
            if (messagesInProcess >= maxMessagesPerRequest) {
                return
            }
            if (curCount < maxMessagesPerRequest) {
                condition.signal()
            }
        }
    }
}