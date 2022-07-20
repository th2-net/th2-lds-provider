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
import mu.KotlinLogging
import java.util.*
import kotlin.math.max

private val LOGGER = KotlinLogging.logger { }

class CustomConfigurationClass(
    val hostname: String? = null,
    val port: Int? = null,
    val keepAliveTimeout: Long? = null,
    val maxBufferDecodeQueue: Int? = null,
    val decodingTimeout: Long? = null,
    val responseQueueSize: Int? = null,
    val execThreadPoolSize: Int? = null,
    val batchSize: Int? = null,
    val mode: String? = null,
    val grpcBackPressure : Boolean? = null,
    val bufferPerQuery: Int? = null,
    val responseFormats: Set<String> = emptySet(),
)

class Configuration(customConfiguration: CustomConfigurationClass) {

    val hostname: String = VariableBuilder.getVariable(customConfiguration::hostname, "localhost")
    val port: Int = VariableBuilder.getVariable(customConfiguration::port, 8080)
    val keepAliveTimeout: Long = VariableBuilder.getVariable(customConfiguration::keepAliveTimeout, 5000)
    val maxBufferDecodeQueue: Int = VariableBuilder.getVariable(customConfiguration::maxBufferDecodeQueue, 10_000)
    val decodingTimeout: Long = VariableBuilder.getVariable(customConfiguration::decodingTimeout, 60_000)
    val responseQueueSize: Int = VariableBuilder.getVariable(customConfiguration::responseQueueSize, 1000)
    val execThreadPoolSize: Int = VariableBuilder.getVariable(customConfiguration::execThreadPoolSize, 10)
    val batchSize: Int = VariableBuilder.getVariable(customConfiguration::batchSize, 100)
    val mode: Mode = VariableBuilder.getVariable(customConfiguration::mode, Mode.HTTP) {
        it.let { Mode.valueOf(it.uppercase(Locale.getDefault())) }
    }
    val grpcBackPressure: Boolean = VariableBuilder.getVariable(customConfiguration::grpcBackPressure, false)
    val bufferPerQuery: Int = VariableBuilder.getVariable(customConfiguration::bufferPerQuery, max(maxBufferDecodeQueue / execThreadPoolSize, 1))
    val responseFormats: Set<ResponseFormat> = VariableBuilder.getVariable(customConfiguration::responseFormats, emptySet()) {
        it.mapTo(hashSetOf(), ResponseFormat.Companion::fromString)
    }
    init {
        require(bufferPerQuery <= maxBufferDecodeQueue) {
            "buffer per queue ($bufferPerQuery) must be less or equal to the total buffer size ($maxBufferDecodeQueue)"
        }
        val batchBoundary = bufferPerQuery.takeIf { it > 0 } ?: maxBufferDecodeQueue
        require(batchSize <= batchBoundary) {
            "bath size ($batchSize) must be less or equal to $batchBoundary (${if (batchBoundary == bufferPerQuery) "bufferPerQuery" else "maxBufferDecodeQueue"})"
        }
        if (mode == Mode.GRPC && grpcBackPressure) {
            LOGGER.warn { "gRPC backpressure works only with ${Mode.GRPC} mode but current mode is $mode" }
        }
    }
}

enum class Mode {
    HTTP, GRPC
}