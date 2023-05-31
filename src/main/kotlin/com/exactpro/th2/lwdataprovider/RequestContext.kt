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
import java.util.concurrent.CompletableFuture

class RequestedMessageDetails(
    val storedMessage: StoredMessage,
    private val onResponse: ((RequestedMessageDetails) -> Unit)? = null
) {
    val id: String = storedMessage.id.toString()
    val rawMessage: RawMessage = RawMessage.parseFrom(storedMessage.content).toBuilder()
        .clearParentEventId() // we remove the parent event ID to avoid generating new events for the original parent
        .build()
    @Volatile
    var time: Long = 0
    var parsedMessage: List<Message>? = null
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

    private fun complete() {
        completed.complete(RequestedMessage(id, storedMessage, rawMessage, parsedMessage))
    }
}

class RequestedMessage(
    val id: String,
    val storedMessage: StoredMessage,
    val rawMessage: RawMessage,
    val parsedMessage: List<Message>?,
)