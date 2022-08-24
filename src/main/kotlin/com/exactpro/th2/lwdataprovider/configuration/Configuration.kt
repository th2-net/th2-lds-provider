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

package com.exactpro.th2.lwdataprovider.configuration

import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import java.util.*


class CustomConfigurationClass {
    var hostname: String? = null
    var port: Int? = null
    val keepAliveTimeout: Long? = null
    val maxBufferDecodeQueue: Int? = null
    val decodingTimeout: Long? = null
    val responseQueueSize: Int? = null
    val execThreadPoolSize: Int? = null
    val batchSize: Int? = null
    val mode: String? = null
    val grpcBackPressure : Boolean? = null
    val bufferPerQuery: Int? = null
    val codecUsePinAttributes: Boolean = true
    val defaultResponseFormats: List<ResponseFormat>? = null
}

class Configuration(customConfiguration: CustomConfigurationClass) {

    val hostname: String = VariableBuilder.getVariable("hostname", customConfiguration.hostname, "localhost")
    val port: Int = VariableBuilder.getVariable("port", customConfiguration.port, 8080)
    val keepAliveTimeout: Long = VariableBuilder.getVariable("keepAliveTimeout", customConfiguration.keepAliveTimeout, 5000)
    val maxBufferDecodeQueue: Int = VariableBuilder.getVariable("maxBufferDecodeQueue", customConfiguration.maxBufferDecodeQueue, 10_000)
    val decodingTimeout: Long = VariableBuilder.getVariable("decodingTimeout", customConfiguration.decodingTimeout, 60_000)
    val responseQueueSize: Int = VariableBuilder.getVariable("responseQueueSize", customConfiguration.responseQueueSize, 1000)
    val execThreadPoolSize: Int = VariableBuilder.getVariable("execThreadPoolSize", customConfiguration.execThreadPoolSize, 10)
    val batchSize: Int = VariableBuilder.getVariable("batchSize", customConfiguration.batchSize, 100)
    val mode: Mode = VariableBuilder.getVariable("mode",
        customConfiguration.mode?.let { Mode.valueOf(it.uppercase(Locale.getDefault())) }, Mode.HTTP)
    val grpcBackPressure: Boolean = VariableBuilder.getVariable("grpcBackPressure", customConfiguration.grpcBackPressure, false)
    val bufferPerQuery: Int = VariableBuilder.getVariable("bufferPerQuery", customConfiguration.bufferPerQuery, 0)
    val codecUsePinAttributes: Boolean = VariableBuilder.getVariable("codecUsePinAttributes", customConfiguration.codecUsePinAttributes, true)
    val defaultResponseFormats: List<ResponseFormat> = VariableBuilder.getVariable("defaultResponseFormats",
        customConfiguration.defaultResponseFormats, listOf(ResponseFormat.ALL))
}

enum class Mode {
    HTTP, GRPC
}