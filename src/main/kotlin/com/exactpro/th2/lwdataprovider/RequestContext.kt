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
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import com.exactpro.th2.lwdataprovider.demo.toDemoRawMessage
import com.exactpro.th2.lwdataprovider.grpc.toRawMessage
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class RequestedMessageDetails(
    val storedMessage: StoredMessage,
    val group: String? = null,
    private val onResponse: ((RequestedMessageDetails) -> Unit)? = null
) {
    val id: String = storedMessage.id.toString()
    val rawMessage: RawMessage by lazy(storedMessage::toRawMessage)
    val demoRawMessage: DemoRawMessage by lazy(storedMessage::toDemoRawMessage)
    @Volatile
    var time: Long = 0
    var parsedMessage: List<Message>? = null
    var demoParsedMessage: List<DemoParsedMessage>? = null
    private val completed = CompletableFuture<RequestedMessage>()
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
        //FIXME: raw message initialized independently
        completed.complete(RequestedMessage(id, storedMessage, rawMessage, parsedMessage, demoParsedMessage))
    }
}

class RequestedMessage(
    val id: String,
    val storedMessage: StoredMessage,
    val rawMessage: RawMessage,
    val parsedMessage: List<Message>?,
    val demoParsedMessage: List<DemoParsedMessage>?,
)