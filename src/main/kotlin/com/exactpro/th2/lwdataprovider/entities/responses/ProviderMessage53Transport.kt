/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import kotlinx.serialization.Serializable
import java.time.Instant
import java.util.Collections

@Deprecated("same format as rpt-data-provider5.3")
@Serializable
data class ProviderMessage53Transport constructor(
    @Serializable(with = InstantSerializer::class) val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
//    val messageType: String?,

    val attachedEventIds: Set<String>,


    val body: List<TransportMessageContainer>?,

    val bodyBase64: String?,

    @Serializable(with = StoredMessageIdSerializer::class)
    val messageId: StoredMessageId,
) : ResponseMessage {

    constructor(
        rawStoredMessage: StoredMessage,
        sessionGroup: String,
        body: List<ParsedMessage>?,
        base64Body: String?,
        events: Set<String> = Collections.emptySet()
    ) : this(
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        direction = Direction.fromStored(rawStoredMessage.direction ?: FIRST),
        sessionId = rawStoredMessage.sessionAlias ?: "",
//        messageType = body?.type,
        attachedEventIds = events,
        body = body?.map { TransportMessageContainer(sessionGroup, it) },
        bodyBase64 = base64Body,
        messageId = rawStoredMessage.id
    )
}
