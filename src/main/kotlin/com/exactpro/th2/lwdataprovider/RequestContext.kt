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

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import io.prometheus.client.Histogram
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

abstract class RequestContext(
   open val channelMessages: ResponseHandler,
   val requestParameters: Map<String, Any> = emptyMap(),
   val counter: AtomicLong = AtomicLong(0L),
   val scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) {

   @Volatile var contextAlive: Boolean = true

   companion object {
      private val logger = KotlinLogging.logger { }
   }
   
   fun finishStream() {
      channelMessages.finishStream()
   }

   fun writeErrorMessage(text: String) {
      logger.info { text }
      channelMessages.writeErrorMessage(text)
   }

   fun keepAliveEvent() {
      channelMessages.keepAliveEvent(scannedObjectInfo, counter);
   }

   open fun onMessageSent() {

   }
}

abstract class MessageRequestContext (
   channelMessages: ResponseHandler,
   requestParameters: Map<String, Any> = emptyMap(),
   counter: AtomicLong = AtomicLong(0L),
   scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo(),
   val requestedMessages: MutableMap<String, RequestedMessageDetails> = ConcurrentHashMap(),
   val streamInfo: ProviderStreamInfo = ProviderStreamInfo(),
   val maxMessagesPerRequest: Int = 0
) : RequestContext(channelMessages, requestParameters, counter, scannedObjectInfo) {

   val lock: ReentrantLock = ReentrantLock()
   val condition: Condition = lock.newCondition()
   val messagesInProcess = AtomicInteger(0)

   val allMessagesRequested: AtomicBoolean = AtomicBoolean(false)
   var loadedMessages = 0

   fun registerMessage(message: RequestedMessageDetails) {
      requestedMessages[message.id] = message
   }

   fun allDataLoadedFromCradle() = allMessagesRequested.set(true)

    abstract fun createMessageDetails(id: String, time: Long, storedMessage: StoredMessage, responseFormats: List<String>, onResponse: () -> Unit = {}): RequestedMessageDetails;
   abstract fun addStreamInfo();

   override fun onMessageSent() {
      if (maxMessagesPerRequest > 0 && messagesInProcess.decrementAndGet() < maxMessagesPerRequest) {
         lock.withLock {
            condition.signal()
         }
      }
   }

    fun startStep(name: String): StepHolder {
        return StepHolder(name, METRICS.labels(name).startTimer())
    }

    companion object {
        private val METRICS = Histogram.build(
            "message_pipeline_hist_time", "Time spent on each step for a message"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .labelNames("step")
            .register()
    }
}

class StepHolder(
    private val name: String,
    private val timer: Histogram.Timer
) : AutoCloseable {
    init {
        LOGGER.trace { "Step $name started with timer ${timer.hashCode()}" }
    }
    private var finished: Boolean = false
    fun finish() {
        if (finished) {
            return
        }
        LOGGER.trace { "Step $name finished with timer ${timer.hashCode()}" }
        finished = true
        timer.observeDuration()
    }
    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }

    override fun close() = finish()
}

abstract class RequestedMessageDetails (
   val id: String,
   @Volatile var time: Long,
   val storedMessage: StoredMessage,
   protected open val context: MessageRequestContext,
   val responseFormats: List<String>,
   var parsedMessage: List<Message>? = null,
   var rawMessage: RawMessage? = null,
   private val onResponse: () -> Unit = {}
) {

   fun responseMessage() {
       try {
           responseMessageInternal()
       } finally {
           onResponse()
       }
   }
   abstract fun responseMessageInternal();

   fun notifyMessage() {
      context.apply { 
         requestedMessages.remove(id)
         scannedObjectInfo.update(id, Instant.now(), counter)
         if (requestedMessages.isEmpty() && allMessagesRequested.get()) {
            addStreamInfo()
            finishStream()
         }
      }
   }
}