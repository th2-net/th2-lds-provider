/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.lwdataprovider.metrics

import com.exactpro.th2.lwdataprovider.metrics.CradleSearchMessageMethod.Companion.LABEL
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

const val INTERFACE_LABEL = "interface"
const val REQUEST_ID_LABEL = "request_id"

enum class CradleSearchMessageMethod {
    SINGLE_MESSAGE,
    MESSAGES,
    MESSAGES_FROM_GROUP;

    companion object {
        const val LABEL = "cradle_search_message_method"
    }
}

val MAX_DECODE_BUFFER_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_max_decode_message_buffer_size")
    .help("Max decode message buffer capacity. It is common buffer for all requests to decode messages")
    .register()

val DECODE_BUFFER_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_decode_message_buffer_size")
    .help("Actual number of raw message in decode buffer.")
    .register()

val MAX_RESPONSE_BUFFER_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_max_response_buffer_size")
    .help("Max message/event response buffer capacity.")
    .register()

//TODO: implement
val RESPONSE_BUFFER_SIZE_GAUGE: Gauge = Gauge.build()
    .name("th2_ldp_response_buffer_size")
    .help("Actual number of message/event in response buffer.")
    .labelNames(REQUEST_ID_LABEL)
    .register()

val CRADLE_BATCH_PROCESS_TIME_COUNTER: Counter = Counter.build()
    .name("th2_ldp_cradle_batch_process_time_seconds")
    .help("Time is seconds which LwDP process cradle batch")
    .labelNames(REQUEST_ID_LABEL)
    .register()

val LOAD_MESSAGES_FROM_CRADLE_COUNTER: Counter = Counter.build()
    .name("th2_ldp_load_messages_from_cradle_total")
    .help("Number of messages loaded from cradle")
    .labelNames(REQUEST_ID_LABEL, LABEL)
    .register()

//TODO: add method label
val LOAD_EVENTS_FROM_CRADLE_COUNTER: Counter = Counter.build()
    .name("th2_ldp_load_events_from_cradle_total")
    .help("Number of events loaded from cradle")
    .labelNames(REQUEST_ID_LABEL)
    .register()

val SEND_MESSAGES_COUNTER: Counter = Counter.build()
    .name("th2_ldp_send_messages_total")
    .help("Send messages via gRPC/HTTP interface")
    .labelNames(REQUEST_ID_LABEL, INTERFACE_LABEL, LABEL)
    .register()

//TODO: implement for sse interface
val SEND_EVENTS_COUNTER: Counter = Counter.build()
    .name("th2_ldp_send_events_total")
    .help("Send events via gRPC/HTTP interface")
    .labelNames(REQUEST_ID_LABEL, INTERFACE_LABEL)
    .register()