/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.cradle.utils.TimeUtils
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.responses.ser.numberOfDigits
import io.javalin.http.util.JsonEscapeUtil
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Base64
import kotlin.text.Charsets.UTF_8
import com.exactpro.cradle.utils.EscapeUtils.escape as cradleEscape

private val COMMA = ",".toByteArray(UTF_8).first().toInt()
private val COLON = ":".toByteArray(UTF_8).first().toInt()
private val ZERO = "0".toByteArray(UTF_8).first().toInt()
private val NULL = "null".toByteArray(UTF_8)
private val TRUE = "true".toByteArray(UTF_8)
private val FALSE = "false".toByteArray(UTF_8)
private val OPENING_CURLY_BRACE = "{".toByteArray(UTF_8).first().toInt()
private val CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first().toInt()
private val OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first().toInt()
private val CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first().toInt()
private val GREATER_THAN = ">".toByteArray(UTF_8).first().toInt()
private val DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first().toInt()

private val TIMESTAMP_FILED = """"timestamp"""".toByteArray(UTF_8)
private val EPOCH_SECOND_FILED = """"epochSecond"""".toByteArray(UTF_8)
private val NANO_FILED = """"nano"""".toByteArray(UTF_8)
private val DIRECTION_FILED = """"direction"""".toByteArray(UTF_8)
private val SESSION_ID_FILED = """"sessionId"""".toByteArray(UTF_8)
private val MESSAGE_ID_FILED = """"messageId"""".toByteArray(UTF_8)
private val ATTACHED_EVENT_IDS_FILED = """"attachedEventIds"""".toByteArray(UTF_8)
private val BODY_FILED = """"body"""".toByteArray(UTF_8)
private val BODY_BASE_64_FILED = """"bodyBase64"""".toByteArray(UTF_8)
private val METADATA_FILED = """"metadata"""".toByteArray(UTF_8)
private val SUBSEQUENCE_FILED = """"subsequence"""".toByteArray(UTF_8)
private val MESSAGE_TYPE_FILED = """"messageType"""".toByteArray(UTF_8)
private val PROPERTIES_FILED = """"properties"""".toByteArray(UTF_8)
private val PROTOCOL_FILED = """"protocol"""".toByteArray(UTF_8)
private val FIELDS_FILED = """"fields"""".toByteArray(UTF_8)

private val EVENT_ID_FILED = """"eventId"""".toByteArray(UTF_8)
private val BATCH_ID_FILED = """"batchId"""".toByteArray(UTF_8)
private val IS_BATCHED_FILED = """"isBatched"""".toByteArray(UTF_8)
private val EVENT_NAME_FILED = """"eventName"""".toByteArray(UTF_8)
private val EVENT_TYPE_FILED = """"eventType"""".toByteArray(UTF_8)
private val END_TIMESTAMP_FILED = """"endTimestamp"""".toByteArray(UTF_8)
private val START_TIMESTAMP_FILED = """"startTimestamp"""".toByteArray(UTF_8)
private val PARENT_EVENT_ID_FILED = """"parentEventId"""".toByteArray(UTF_8)
private val SUCCESSFUL_FILED = """"successful"""".toByteArray(UTF_8)
private val BOOK_ID_FILED = """"bookId"""".toByteArray(UTF_8)
private val SCOPE_FILED = """"scope"""".toByteArray(UTF_8)
private val ATTACHED_MESSAGE_IDS_FILED = """"attachedMessageIds"""".toByteArray(UTF_8)

private val TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSSSSS")
    .withZone(ZoneOffset.UTC)
private val ESCAPE_CHARACTERS = charArrayOf('\"', '\n', '\r', '\\', '\t', '\b')

fun ProviderMessage53Transport.toJSONByteArray(): ByteArray =
    ByteArrayOutputStream(1_024 * 2).apply { // TODO: init size
        write(OPENING_CURLY_BRACE)
        writeTimestamp(TIMESTAMP_FILED, timestamp)
        write(COMMA)
        direction?.let {
            writeField(DIRECTION_FILED, direction.name)
            write(COMMA)
        }
        writeField(SESSION_ID_FILED, sessionId)
        write(COMMA)
        writeAttachedEventIds(attachedEventIds)
        body?.let {
            write(COMMA)
            writeBody(body)
        }
        bodyBase64?.let {
            write(COMMA)
            writeFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64)
        }
        write(COMMA)
        writeMessageId(messageId)
        write(CLOSING_CURLY_BRACE)
    }.toByteArray()

fun Event.toJSONByteArray(): ByteArray =
    ByteArrayOutputStream(1_024 * 2).apply { // TODO: init size
        write(OPENING_CURLY_BRACE)
        writeField(EVENT_ID_FILED, eventId)
        write(COMMA)
        batchId?.let { writeField(BATCH_ID_FILED, it) } ?: run { writeNull(BATCH_ID_FILED) }
        write(COMMA)
        writeField(IS_BATCHED_FILED, isBatched)
        write(COMMA)
        writeField(EVENT_NAME_FILED, eventName)
        write(COMMA)
        eventType?.let { writeField(EVENT_TYPE_FILED, it) } ?: run { writeNull(EVENT_TYPE_FILED) }
        write(COMMA)
        endTimestamp?.let { writeTimestamp(END_TIMESTAMP_FILED, it) } ?: run { writeNull(END_TIMESTAMP_FILED) }
        write(COMMA)
        writeTimestamp(START_TIMESTAMP_FILED, startTimestamp)
        write(COMMA)
        parentEventId?.let { writeBatchParentEventId(PARENT_EVENT_ID_FILED, it) } ?: run { writeNull(PARENT_EVENT_ID_FILED) }
        write(COMMA)
        writeField(SUCCESSFUL_FILED, successful)
        write(COMMA)
        writeField(BOOK_ID_FILED, bookId)
        write(COMMA)
        writeField(SCOPE_FILED, scope)
        write(COMMA)
        if (attachedMessageIds.isNotEmpty()) {
            writeStringList(ATTACHED_MESSAGE_IDS_FILED, attachedMessageIds)
        } else {
            writeEmptyList(ATTACHED_MESSAGE_IDS_FILED)
        }
        write(COMMA)
        if (body != null && body.isNotEmpty()) {
            writeBody(BODY_FILED, body)
        } else {
            writeEmptyList(BODY_FILED)
        }
        write(CLOSING_CURLY_BRACE)
    }.toByteArray()

private fun jsonEscape(value: String): String {
    return if (ESCAPE_CHARACTERS.any(value::contains)) {
        JsonEscapeUtil.escape(value)
    } else {
        value
    }
}

private fun OutputStream.writeMessageId(messageId: StoredMessageId) {
    write(MESSAGE_ID_FILED)
    write(COLON)
    write(DOUBLE_QUOTE)
    with(messageId) {
        write(jsonEscape(cradleEscape(bookId.toString())).toByteArray(UTF_8))
        write(COLON)
        write(jsonEscape(cradleEscape(sessionAlias)).toByteArray(UTF_8))
        write(COLON)
        write(direction.label.toByteArray(UTF_8))
        write(COLON)
        TimeUtils.toLocalTimestamp(timestamp).apply {
            writeNumber(year, 4)
            writeTwoDigits(monthValue)
            writeTwoDigits(dayOfMonth)
            writeTwoDigits(hour)
            writeTwoDigits(minute)
            writeTwoDigits(second)
            writeNumber(nano, 9)
        }
        write(COLON)
        write(sequence.toString().toByteArray(UTF_8))
    }
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeBody(messages: List<TransportMessageContainer>) {
    write(BODY_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    messages.forEachIndexed { index, message ->
        val parsedMessage = message.parsedMessage
        if (!parsedMessage.rawBody.isReadable) {
            error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
        }
        if (index != 0) {
            write(COMMA)
        }
        write(OPENING_CURLY_BRACE)
        writeMetadata(parsedMessage)
        write(COMMA)
        writeFields(parsedMessage)
        write(CLOSING_CURLY_BRACE)
    }
    write(CLOSING_SQUARE_BRACE)
}

private fun OutputStream.writeFields(parsedMessage: ParsedMessage) {
    write(FIELDS_FILED)
    write(COLON)
    write(parsedMessage.rawBody.toByteArray())
}

private fun OutputStream.writeMetadata(message: ParsedMessage) {
    write(METADATA_FILED)
    write(COLON)
    write(OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            writeNumberList(SUBSEQUENCE_FILED, id.subsequence)
            write(COMMA)
        }
        writeField(MESSAGE_TYPE_FILED, type)
        if (metadata.isNotEmpty()) {
            write(COMMA)
            writeMap(PROPERTIES_FILED, metadata)
        }
        if (protocol.isNotBlank()) {
            write(COMMA)
            writeField(PROTOCOL_FILED, protocol)
        }
    }

    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeBatchParentEventId(name: ByteArray, batchEventId: ProviderEventId) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    batchEventId.batchId?.let {
        writeEventId(it)
        write(GREATER_THAN)
    }
    writeEventId(batchEventId.eventId)
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeBody(name: ByteArray, value: ByteArray) {
    write(name)
    write(COLON)
    if (value.first().toInt().let { it == OPENING_SQUARE_BRACE || it == OPENING_CURLY_BRACE }
        && value.last().toInt().let { it == CLOSING_SQUARE_BRACE || it == CLOSING_CURLY_BRACE }) {
        write(value)
    } else {
        write(DOUBLE_QUOTE)
        write(Base64.getEncoder().encode(value))
        write(DOUBLE_QUOTE)
    }
}

private fun OutputStream.writeEventId(eventId: StoredTestEventId) {
    write(jsonEscape(eventId.bookId.name).toByteArray(UTF_8))
    write(COLON)
    write(jsonEscape(eventId.scope).toByteArray(UTF_8))
    write(COLON)
    write(TIMESTAMP_FORMAT.format(LocalDateTime.ofInstant(eventId.startTimestamp, ZoneOffset.UTC)).toByteArray(UTF_8))
    write(COLON)
    write(jsonEscape(eventId.id).toByteArray(UTF_8))
}

private fun OutputStream.writeAttachedEventIds(attachedEventIds: Set<String>) {
    write(ATTACHED_EVENT_IDS_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            write(COMMA)
        }
        write(DOUBLE_QUOTE)
        write(jsonEscape(eventId).toByteArray(UTF_8))
        write(DOUBLE_QUOTE)
    }
    write(CLOSING_SQUARE_BRACE)

}

private fun OutputStream.writeTimestamp(name: ByteArray, timestamp: Instant) {
    write(name)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    writeField(EPOCH_SECOND_FILED, timestamp.epochSecond)
    write(COMMA)
    writeField(NANO_FILED, timestamp.nano)
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeTwoDigits(value: Int) {
    if (value < 10) {
        write(ZERO)
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeNumber(value: Int, size: Int) {
    val digits = numberOfDigits(value)
    if (digits < size) {
        repeat(size - digits) {
            write(ZERO)
        }
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeFieldWithoutEscaping(name: ByteArray, value: String) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(value.toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeField(name: ByteArray, value: String) = writeFieldWithoutEscaping(name, jsonEscape(value))
private fun OutputStream.writeField(name: ByteArray, value: Boolean) {
    write(name)
    write(COLON)
    write(if(value) TRUE else FALSE)
}
private fun OutputStream.writeField(name: ByteArray, value: Number) {
    write(name)
    write(COLON)
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeField(name: String, value: String) {
    write(DOUBLE_QUOTE)
    write(jsonEscape(name).toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(jsonEscape(value).toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeNull(name: ByteArray) {
    write(name)
    write(COLON)
    write(NULL)
}

private fun OutputStream.writeEmptyList(name: ByteArray) {
    write(name)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    write(CLOSING_SQUARE_BRACE)
}

private fun OutputStream.writeMap(name: ByteArray, value: Map<String, String>) {
    write(name)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            write(COMMA)
        }
        writeField(entry.key, entry.value)
    }
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeNumberList(name: ByteArray, value: Collection<Number>) {
    writeList(name, value) { write(it.toString().toByteArray(UTF_8)) }
}

private fun OutputStream.writeStringList(name: ByteArray, value: Collection<String>) {
    writeList(name, value) {
        write(DOUBLE_QUOTE)
        write(jsonEscape(it).toByteArray(UTF_8))
        write(DOUBLE_QUOTE)
    }
}

private fun <T> OutputStream.writeList(name: ByteArray, values: Collection<T>, writeValue: OutputStream.(T) -> Unit) {
    write(name)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    val lastIndex = values.size - 1
    values.forEachIndexed { index, value ->
        writeValue(value)
        if (lastIndex != index) {
            write(COMMA)
        }
    }
    write(CLOSING_SQUARE_BRACE)
}
