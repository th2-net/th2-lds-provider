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

import com.exactpro.th2.lwdataprovider.CradleMessageSource.Companion.CRADLE_MESSAGE_SOURCE_LABEL
import com.exactpro.th2.lwdataprovider.ExposedInterface.Companion.INTERFACE_LABEL
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram

data class Metrics(
    private val histogramTime: Histogram,
    private val gauge: Gauge
) {

    constructor(variableName: String, descriptionName: String) : this(
        histogramTime = Histogram.build(
            "${variableName}_hist_time", "Time of $descriptionName"
        ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
            .register(),
        gauge = Gauge.build(
            "${variableName}_gauge", "Quantity of $descriptionName using Gauge"
        ).register()
    )

    fun startObserve(): Histogram.Timer {
        gauge.inc()
        return histogramTime.startTimer()
    }

    fun stopObserve(timer: Histogram.Timer) {
        gauge.dec()
        timer.observeDuration()
    }
}

enum class ExposedInterface {
    SSE,
    GRPC;

    companion object {
        const val INTERFACE_LABEL = "interface"
    }
}

enum class CradleMessageSource {
    MESSAGE,
    GROUP;

    companion object {
        const val CRADLE_MESSAGE_SOURCE_LABEL = "cradle_message_source"
    }
}

val DECODE_QUEUE_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_buffer_decode_message_queue_size")
    .help("Actual number of raw message in the decode queue")
    .register()

val MAX_DECODE_QUEUE_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_max_buffer_decode_message_queue_total")
    .help("Max decode message queue capacity. It is common buffer for all requests to decode messages")
    .register()

val LOAD_MESSAGES_FROM_CRADLE_COUNTER: Counter = Counter.build()
    .name("th2_ldp_load_messages_from_cradle_total")
    .help("Number of messages loaded from cradle")
    .labelNames(CRADLE_MESSAGE_SOURCE_LABEL)
    .register()

//TODO: add method label
val LOAD_EVENTS_FROM_CRADLE_COUNTER: Counter = Counter.build()
    .name("th2_ldp_load_events_from_cradle_total")
    .help("Number of events loaded from cradle")
    .register()

val SEND_MESSAGES_COUNTER: Counter = Counter.build()
    .name("th2_ldp_send_messages_total")
    .help("Send messages via gRPC/HTTP interface")
    .labelNames(INTERFACE_LABEL, CRADLE_MESSAGE_SOURCE_LABEL)
    .register()

//TODO: implement for sse interface
val SEND_EVENTS_COUNTER: Counter = Counter.build()
    .name("th2_ldp_send_events_total")
    .help("Send events via gRPC/HTTP interface")
    .labelNames(INTERFACE_LABEL)
    .register()