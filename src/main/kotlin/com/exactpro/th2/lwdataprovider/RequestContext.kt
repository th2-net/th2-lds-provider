/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.lwdataprovider.transport.toRawMessage
import com.exactpro.th2.lwdataprovider.grpc.toRawMessage
import com.exactpro.th2.lwdataprovider.metrics.DecodingMetrics
import com.exactpro.th2.lwdataprovider.workers.CradleRequestId
import com.exactpro.th2.lwdataprovider.workers.RequestId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class RequestedMessageDetails(
    val storedMessage: StoredMessage,
    val sessionGroup: String? = null,
    private val onResponse: ((RequestedMessageDetails) -> Unit)? = null
) {
    val requestId: RequestId = CradleRequestId(storedMessage.id)
    val protoRawMessage: Lazy<ProtoRawMessage> = lazy(LazyThreadSafetyMode.NONE, storedMessage::toRawMessage)
    val transportRawMessage: Lazy<RawMessage> = lazy(LazyThreadSafetyMode.NONE, storedMessage::toRawMessage)
    @Volatile
    var time: Long = 0
    var protoParsedMessages: List<Message>? = null
    var transportParsedMessages: List<ParsedMessage>? = null
    val completed = CompletableFuture<RequestedMessage>()
    init {
        if (onResponse == null) {
            // nothing to await
            complete()
        }
    }

    fun responseMessage() {
        onResponse?.invoke(this)
        complete()
    }

    fun awaitAndGet(): RequestedMessage = completed.get()

    // TODO: use to get error if no response during timeout
    fun awaitAndGet(timeout: Long, unit: TimeUnit): RequestedMessage? = try {
        completed.get(timeout, unit)
    } catch (ex: TimeoutException) {
        null
    }

    private fun complete() {
        completed.complete(RequestedMessage(requestId, sessionGroup ?: "", storedMessage, protoRawMessage, protoParsedMessages, transportParsedMessages))
        DecodingMetrics.incDecoded()
    }
}

class RequestedMessage(
    val requestId: RequestId,
    val sessionGroup: String,
    val storedMessage: StoredMessage,
    val rawMessage: Lazy<com.exactpro.th2.common.grpc.RawMessage>,
    val protoMessage: List<Message>?,
    val transportMessage: List<ParsedMessage>?,
)