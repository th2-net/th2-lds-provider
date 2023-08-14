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

package com.exactpro.th2.lwdataprovider.metrics

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram

object DecodingMetrics {
    private val decoded = Counter.build(
        "th2_ldp_decoded_total", "Number decoded messages"
    ).register()

    private val messagesWaiting: Gauge = Gauge.build(
        "th2_ldp_wait_decode",
        "number of messages waiting to decode",
    ).register()

    private val maxMessageWaiting: Gauge = Gauge.build(
        "th2_ldp_wait_decode_max",
        "max number of messages waiting to decode",
    ).register()

    private val maxDecodeTime: Gauge = Gauge.build(
        "th2_ldp_decode_time_max",
        "the decode timeout"
    ).register()

    private val decodingTime: Histogram = Histogram.build(
        "th2_ldp_decode_time",
        "how long the messages were decoded by codecs"
    ).labelNames("marker")
        .buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 75.0)
        .register()

    fun setMaxDecodeTimeout(seconds: Int) {
        maxDecodeTime.set(seconds.toDouble())
    }

    fun setMaxWaitingQueue(size: Int) {
        maxMessageWaiting.set(size.toDouble())
    }

    fun currentWaiting(size: Int) {
        messagesWaiting.set(size.toDouble())
    }

    fun startTimer(marker: String): AutoCloseable {
        return decodingTime.labels(marker).startTimer()
    }

    fun incDecoded() {
        decoded.inc()
    }
}