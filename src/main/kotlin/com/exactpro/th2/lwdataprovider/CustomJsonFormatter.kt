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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.google.gson.Gson

class CustomJsonFormatter  {
    
    private val sb1 : StringBuilder = StringBuilder()
    
    companion object {
        private const val QUOTE_CHAR = '"'.code
    }
    
    fun print(msg : Message) : String {
        sb1.setLength(0)
        printM(msg, sb1)
        return sb1.toString()
    }

    private fun printM (msg: Message, sb: StringBuilder) {
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

    private val gson = Gson()
    
    private fun printV (value: Value, sb: StringBuilder) {
        when (value.kindCase) {
            Value.KindCase.SIMPLE_VALUE -> {
                sb.append("{\"simpleValue\":")
                convertStringToJson(value.simpleValue, sb)
                sb.append('}')
            }
            Value.KindCase.LIST_VALUE -> {
                sb.append("{\"listValue\":{\"values\":[")
                val valuesList = value.listValue.valuesList
                if (valuesList.isNotEmpty()) {
                    valuesList.forEach {
                        printV(it, sb)
                        sb.append(',')
                    }
                    sb.setLength(sb.length - 1)
                }
                sb.append("]}}")
            }
            Value.KindCase.MESSAGE_VALUE -> {
                sb.append("{\"messageValue\":")
                printM(value.messageValue, sb)
                sb.append('}')
            }
            Value.KindCase.NULL_VALUE -> {
                sb.append("{\"nullValue\": {}")
            }
            else -> {
            }
        }
    }

    private fun isNeedToEscape(s: String) : Boolean {
        // ascii 32 is space, all chars below should be escaped
        return s.chars().anyMatch { it < 32 || it == QUOTE_CHAR }
    }
    
    private fun convertStringToJson(s: String, builder: StringBuilder) {
        if (isNeedToEscape(s)) {
            gson.toJson(s, builder)
        } else {
            builder.append('"').append(s).append('"')
        }
    }

    private fun printMetadata (msg: MessageMetadata, sb: StringBuilder) {
        sb.append("\"metadata\":{")
        var first = true
        if (msg.hasId()) {
            sb.append("\"id\":{")
            val id = msg.id
            if (id.hasConnectionId()) {
                sb.append("\"connectionId\":{\"sessionAlias\":\"").append(id.connectionId.sessionAlias).append("\"},")
            }
            sb.append("\"direction\":\"").append(id.direction.name).append("\",");
            sb.append("\"sequence\":\"").append(id.sequence).append("\",");
            sb.append("\"subsequence\":[")
            if (id.subsequenceCount > 0) {
                id.subsequenceList.forEach { sb.append(it.toString()).append(',') }
                sb.setLength(sb.length - 1)
            }
            sb.append("]}")
            first = false
        }
        if (msg.hasTimestamp()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"timestamp\":{\"seconds\":\"").append(msg.timestamp.seconds).append("\",\"nanos\":\"")
                .append(msg.timestamp.nanos).append("\"}")
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
}