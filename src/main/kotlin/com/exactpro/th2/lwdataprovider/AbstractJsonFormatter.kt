/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.lwdataprovider.demo.toProtoDirection
import com.exactpro.th2.lwdataprovider.producers.JsonFormatter
import com.google.gson.Gson
import java.time.Instant

abstract class AbstractJsonFormatter : JsonFormatter {
    private val sb1: StringBuilder = StringBuilder()
    private val gson = Gson()
    override fun print(message: Message): String {
        sb1.setLength(0)
        printM(message, sb1)
        return sb1.toString()
    }

    override fun print(message: DemoParsedMessage): String {
        sb1.setLength(0)
        printDM(message, sb1)
        return sb1.toString()
    }

    protected abstract fun printV(value: Value, sb: StringBuilder)

    protected abstract fun printDV(value: Any?, sb: StringBuilder)

    protected fun printM(msg: Message, sb: StringBuilder) {
        sb.append("{")
        if (msg.hasMetadata()) {
            printMetadata(msg.metadata, sb)
            sb.append(',')
        }
        sb.append("\"fields\":{")
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                sb.append('"').append(entry.key).append("\":")
                printV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}').append('}')
    }

    protected fun printM(msg: Map<*, *>, sb: StringBuilder) {
        sb.append("{")
        sb.append("\"fields\":{")
        if (msg.isNotEmpty()) {
            for (entry in msg.entries) {
                sb.append('"').append(entry.key).append("\":")
                printDV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}').append('}')
    }

    private fun printDM(msg: DemoParsedMessage, sb: StringBuilder) {
        sb.append("{")
        printDemoMetadata(msg, sb)
        sb.append(',')
        sb.append("\"fields\":{")
        if (msg.body.isNotEmpty()) {
            for (entry in msg.body.entries) {
                sb.append('"').append(entry.key).append("\":")
                printDV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}').append('}')
    }

    private fun isNeedToEscape(s: String): Boolean {
        // ascii 32 is space, all chars below should be escaped
        return s.chars().anyMatch { it < 32 || it == CustomProtoJsonFormatter.QUOTE_CHAR || it == CustomProtoJsonFormatter.BACK_SLASH }
    }

    protected fun StringBuilder.escapeAndAppend(value: String): StringBuilder = if (isNeedToEscape(value)) {
        this.apply {
            val start = length
            append(value)
            val end = lastIndex
            for (i: Int in end downTo start) {
                REPLACEMENT_CHARS.getOrNull(get(i).code)?.let { replacement ->
                    replace(i, i + 1, replacement)
                }
            }
        }
    } else {
        append(value)
    }

    private fun printMetadata(msg: MessageMetadata, sb: StringBuilder) {
        sb.append("\"metadata\":{")
        var first = true
        if (msg.hasId()) {
            sb.append("\"id\":{")
            val id = msg.id
            if (id.hasConnectionId()) {
                sb.append("\"connectionId\":{\"sessionAlias\":\"").append(id.connectionId.sessionAlias).append("\"},")
            }
            sb.append("\"direction\":\"").append(id.direction.name).append("\",")
            sb.append("\"sequence\":\"").append(id.sequence).append("\",")
            if (id.hasTimestamp()) {
                sb.append("\"timestamp\":{\"seconds\":\"").append(id.timestamp.seconds).append("\",\"nanos\":\"")
                    .append(id.timestamp.nanos).append("\"},")
            }
            sb.append("\"subsequence\":[")
            if (id.subsequenceCount > 0) {
                id.subsequenceList.forEach { sb.append(it.toString()).append(',') }
                sb.setLength(sb.length - 1)
            }
            sb.append("]}")
            first = false
        }
        if (msg.messageType.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"messageType\":\"").append(msg.messageType).append("\"")
            first = false
        }
        if (msg.propertiesCount > 0) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"properties\":")
            gson.toJson(msg.propertiesMap, sb)
            first = false
        }
        if (msg.protocol.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"protocol\":\"").append(msg.protocol).append("\"")
        }
        sb.append("}")
    }

    private fun printDemoMetadata(msg: DemoParsedMessage, sb: StringBuilder) {
        sb.append("\"metadata\":{")
        var first = true
        if (msg.id !== DemoMessageId.DEFAULT_INSTANCE) {
            sb.append("\"id\":{")
            val id = msg.id
            if (id.sessionAlias.isNotEmpty() || id.sessionGroup.isNotEmpty()) {
                sb.append("\"connectionId\":{")
                if (id.sessionGroup.isNotEmpty()) {
                    sb.append("\"sessionGroup\":\"").append(id.sessionGroup).append("\"")
                    if (id.sessionAlias.isNotEmpty()) {
                        sb.append(",")
                    }
                }
                if (id.sessionAlias.isNotEmpty()) {
                    sb.append("\"sessionAlias\":\"").append(id.sessionAlias).append("\"")
                }
                sb.append("},")
            }
            sb.append("\"direction\":\"").append(id.direction.toProtoDirection().name).append("\",")
            sb.append("\"sequence\":\"").append(id.sequence).append("\",")
            if (id.timestamp !== Instant.EPOCH) {
                sb.append("\"timestamp\":{\"seconds\":\"").append(id.timestamp.epochSecond).append("\",\"nanos\":\"")
                    .append(id.timestamp.nano).append("\"},")
            }
            sb.append("\"subsequence\":[")
            if (id.subsequence.isNotEmpty()) {
                id.subsequence.forEach { sb.append(it.toString()).append(',') }
                sb.setLength(sb.length - 1)
            }
            sb.append("]}")
            first = false
        }
        if (msg.type.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"messageType\":\"").append(msg.type).append("\"")
            first = false
        }
        if (msg.metadata.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"properties\":")
            gson.toJson(msg.metadata, sb)
            first = false
        }
        if (msg.protocol.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"protocol\":\"").append(msg.protocol).append("\"")
        }
        sb.append("}")
    }

    companion object {
        // This code has been copied from com.google.gson.stream.JsonWriter to improve performance of escaping
        private var REPLACEMENT_CHARS: Array<String?> = arrayOfNulls<String>(128).apply {
            for (i in 0..0x1f) {
                set(i, String.format("\\u%04x", i))
            }
            set('"'.code, "\\\"")
            set('\\'.code, "\\\\")
            set('\t'.code, "\\t")
            set('\b'.code, "\\b")
            set('\n'.code, "\\n")
            set('\r'.code, "\\r")
            set('\u000c'.code, "\\u000c")
        }
    }
}