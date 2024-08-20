/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.configuration

import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.max
import kotlin.math.min

private val LOGGER = KotlinLogging.logger { }

class CustomConfigurationClass(
    val hostname: String? = null,
    val port: Int? = null,
    val keepAliveTimeout: Long? = null,
    val maxBufferDecodeQueue: Int? = null,
    val decodingTimeout: Long? = null,
    val responseQueueSize: Int? = null,
    val execThreadPoolSize: Int? = null,
    val convThreadPoolSize: Int? = null,
    val mode: String? = null,
    val grpcBackPressure : Boolean? = null,
    val bufferPerQuery: Int? = null,
    val responseFormats: Set<String>? = null,
    val grpcBackPressureReadinessTimeoutMls: Long? = null,
    val codecUsePinAttributes: Boolean? = null,
    val listOfMessageAsSingleMessage: Boolean? = null,
    val useTransportMode: Boolean? = null,
    val validateCradleData: Boolean? = null,
    val flushSseAfter: Int? = null,
    val gzipCompressionLevel: Int? = null,
    @JsonDeserialize(using = ByteSizeDeserializer::class)
    val batchSizeBytes: Int? = null,
    val downloadTaskTTL: Long? = null,
)

class Configuration(customConfiguration: CustomConfigurationClass) {

    val hostname: String = VariableBuilder.getVariable(customConfiguration::hostname, "localhost")
    val port: Int = VariableBuilder.getVariable(customConfiguration::port, 8080)
    val keepAliveTimeout: Long = VariableBuilder.getVariable(customConfiguration::keepAliveTimeout, 5000)
    val maxBufferDecodeQueue: Int = VariableBuilder.getVariable(customConfiguration::maxBufferDecodeQueue, 10_000)
    val decodingTimeout: Long = VariableBuilder.getVariable(customConfiguration::decodingTimeout, 60_000)
    val responseQueueSize: Int = VariableBuilder.getVariable(customConfiguration::responseQueueSize, 1000)
    val execThreadPoolSize: Int = VariableBuilder.getVariable(customConfiguration::execThreadPoolSize, 10)
    val convThreadPoolSize: Int = VariableBuilder.getVariable(customConfiguration::convThreadPoolSize, 3)
    val batchSize: Int
    val mode: Mode = VariableBuilder.getVariable(customConfiguration::mode, Mode.HTTP) {
        it.let { Mode.valueOf(it.uppercase(Locale.getDefault())) }
    }
    val grpcBackPressure: Boolean = VariableBuilder.getVariable(customConfiguration::grpcBackPressure, false)
    val bufferPerQuery: Int = VariableBuilder.getVariable(customConfiguration::bufferPerQuery, max(maxBufferDecodeQueue / execThreadPoolSize, 1))
    val responseFormats: Set<ResponseFormat> = VariableBuilder.getVariable(customConfiguration::responseFormats, EnumSet.of(ResponseFormat.BASE_64, ResponseFormat.PROTO_PARSED)) {
        it.mapTo(hashSetOf(), ResponseFormat.Companion::fromString)
    }
    val grpcBackPressureReadinessTimeoutMls: Long = VariableBuilder.getVariable(customConfiguration::grpcBackPressureReadinessTimeoutMls, decodingTimeout)
    val codecUsePinAttributes: Boolean = VariableBuilder.getVariable(customConfiguration::codecUsePinAttributes, true)
    val listOfMessageAsSingleMessage: Boolean = VariableBuilder.getVariable(customConfiguration::listOfMessageAsSingleMessage, true)
    val useTransportMode: Boolean = VariableBuilder.getVariable(customConfiguration::useTransportMode, false)
    val validateCradleData: Boolean = VariableBuilder.getVariable(customConfiguration::validateCradleData, false)
    val flushSseAfter: Int = VariableBuilder.getVariable(customConfiguration::flushSseAfter, 0)
    val gzipCompressionLevel: Int = VariableBuilder.getVariable(customConfiguration::gzipCompressionLevel, -1)
    val batchSizeBytes: Int = VariableBuilder.getVariable(customConfiguration::batchSizeBytes, 256 * 1024)
    val downloadTaskTTL: Long = VariableBuilder.getVariable(customConfiguration::downloadTaskTTL, TimeUnit.HOURS.toMillis(1))
    init {
        require(bufferPerQuery <= maxBufferDecodeQueue) {
            "buffer per queue ($bufferPerQuery) must be less or equal to the total buffer size ($maxBufferDecodeQueue)"
        }
        val batchBoundary = bufferPerQuery.takeIf { it > 0 } ?: maxBufferDecodeQueue
        // Max batch size in order to meet the queue limits
        // We also should meet response queue limits - the batch size cannot be greater than the response queue size
        batchSize = min(batchBoundary, responseQueueSize)
        if (mode != Mode.GRPC && grpcBackPressure) {
            LOGGER.warn { "gRPC backpressure works only with ${Mode.GRPC} mode but current mode is $mode" }
        }
        require(grpcBackPressureReadinessTimeoutMls > 0) {
            "grpcBackPressureReadinessTimeoutMls ($grpcBackPressureReadinessTimeoutMls) must be positive"
        }
        require(flushSseAfter >= 0) {
            "flushSseAfter must be positive integer or zero"
        }
        require(gzipCompressionLevel <= 9 && gzipCompressionLevel >= -1) {
            "gzipCompressionLevel must be integer in the [-1, 9] range"
        }
        require(downloadTaskTTL > 0) { "negative download task TTL: $downloadTaskTTL" }
    }

    override fun toString(): String {
        return "Configuration(" +
                "hostname='$hostname', " +
                "port=$port, " +
                "keepAliveTimeout=$keepAliveTimeout, " +
                "maxBufferDecodeQueue=$maxBufferDecodeQueue, " +
                "decodingTimeout=$decodingTimeout, " +
                "responseQueueSize=$responseQueueSize, " +
                "execThreadPoolSize=$execThreadPoolSize, " +
                "convThreadPoolSize=$convThreadPoolSize, " +
                "batchSize=$batchSize, " +
                "mode=$mode, " +
                "grpcBackPressure=$grpcBackPressure, " +
                "bufferPerQuery=$bufferPerQuery, " +
                "responseFormats=$responseFormats, " +
                "grpcBackPressureReadinessTimeoutMls=$grpcBackPressureReadinessTimeoutMls, " +
                "codecUsePinAttributes=$codecUsePinAttributes, " +
                "listOfMessageAsSingleMessage=$listOfMessageAsSingleMessage, " +
                "useTransportMode=$useTransportMode, " +
                "validateCradleData=$validateCradleData, " +
                "flushSseAfter=$flushSseAfter, " +
                "gzipCompressionLevel=$gzipCompressionLevel, " +
                "batchSizeBytes=$batchSizeBytes" +
                ")"
    }
}

enum class Mode {
    HTTP, GRPC
}