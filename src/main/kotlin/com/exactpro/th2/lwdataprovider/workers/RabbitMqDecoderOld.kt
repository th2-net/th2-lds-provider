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

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread

@Deprecated("use 2nd version")
class RabbitMqDecoderOld (private val queue: BlockingQueue<List<RequestedMessageDetails<*>>>, private val configuration: Configuration,
                          private val messageRouterParsedBatch: MessageRouter<MessageGroupBatch>,
                          private val messageRouterRawBatch: MessageRouter<RawMessageBatch>,
                          private val toWriteQueue: BlockingQueue<RequestedMessageDetails<*>>
) {
    
    var running: AtomicBoolean = AtomicBoolean(false)
    var decodeBuffer = DecodeQueueBuffer()
    var lock = ReentrantLock()
    var cond: Condition = lock.newCondition()
    var parsedMonitor = messageRouterParsedBatch.subscribeAll(CodecMessageListener(decodeBuffer), QueueAttribute.PARSED.value, "from_codec")
    
    
    var thread: Thread? = null
    
    fun start() {
        thread = thread(name="rabbit-mq-watcher", start = true) { run() }
    }

    fun run() {

        running.set(true)

        var currStreamName = ""
        var currBatch = RawMessageBatch.newBuilder()
        var batchLength = 0
        
        while (running.get()) {
            val message = queue.poll(200L, TimeUnit.MILLISECONDS)
            
            message?.let {
                val msgStreamName = it[0].storedMessage.streamName
                if (currStreamName != msgStreamName && batchLength > 0) {
                    messageRouterRawBatch.sendAll(currBatch.build(), currStreamName)
                    currBatch = RawMessageBatch.newBuilder()
                    batchLength = 0
                }
                currStreamName = msgStreamName
                batchLength += this.prepareMessageBatch(it, currBatch)
            }

            if (batchLength > 100 || (message == null /*|| decodeBuffer.getSize() >= maxDecodeQueueSize*/) && batchLength > 0) {
                messageRouterRawBatch.sendAll(currBatch.build(), currStreamName)
                currStreamName = ""
                currBatch = RawMessageBatch.newBuilder()
                batchLength = 0
            }
            
            /*if (decodeBuffer.getSize() >= maxDecodeQueueSize) {
                lock.withLock { cond.await() }
            }*/
        }
        
    }
    
    fun stop() {
        running.set(false);
        parsedMonitor?.unsubscribe();
    }

    private fun prepareMessageBatch(messageBatch: List<RequestedMessageDetails<*>>, batch: RawMessageBatch.Builder): Int {
        var batchSize = 0
        messageBatch.forEach {
            batch.addMessages(RawMessage.parseFrom(it.storedMessage.content))
            if (decodeBuffer.add(it)) {
                batch.addMessages(RawMessage.parseFrom(it.storedMessage.content))
                batchSize += messageBatch.size
            }
        }
        return batchSize

    }
    
}