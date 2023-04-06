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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonRawValue
import io.javalin.openapi.OpenApiIgnore
import io.javalin.openapi.OpenApiPropertyType
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import java.time.Instant
import java.util.*

@Deprecated("same format as rpt-data-provider5.3")
@Serializable
data class ProviderMessage53 constructor(
    @Serializable(with = InstantSerializer::class) val timestamp: Instant,
    val direction: Direction?,
    val sessionId: String,
    val messageType: String,

    val attachedEventIds: Set<String>,

    @JsonRawValue
    @get:OpenApiPropertyType(Map::class)
    @Serializable(with = UnwrappingJsonListSerializer::class)
    val body: String?,

    val bodyBase64: String?,

    @JsonIgnore
    @get:OpenApiIgnore
    @Transient
    val id: StoredMessageId = DEFAULT_STORE_ID,

    val messageId: String,
) : ResponseMessage {

    constructor(
        rawStoredMessage: StoredMessage,
        jsonBody: String?,
        base64Body: String?,
        messageType: String,
        events: Set<String> = Collections.emptySet()
    ) : this(
        timestamp = rawStoredMessage.timestamp ?: Instant.ofEpochMilli(0),
        direction = Direction.fromStored(rawStoredMessage.direction ?: FIRST),
        sessionId = rawStoredMessage.sessionAlias ?: "",
        messageType = messageType,
        attachedEventIds = events,
        body = jsonBody ?: "{}",
        bodyBase64 = base64Body,
        id = rawStoredMessage.id,
        messageId = rawStoredMessage.id.toString()
    )

    companion object {
        private val DEFAULT_STORE_ID = StoredMessageId(BookId("default"), "default", FIRST, Instant.MIN, 0)
    }
}
