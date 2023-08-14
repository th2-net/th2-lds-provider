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
import io.prometheus.client.Histogram

object HttpWriteMetrics {
    private val converted = Counter.build(
        "th2_ldp_sse_converted_total", "Number converted messages from requested message details to SSE event"
    ).register()

    private val writingHistogram: Histogram = Histogram.build(
        "th2_ldp_sse_write_time",
        "time spent to write a response to the output"
    ).labelNames("uri")
        .exponentialBuckets(0.000000001, 10.0, 10)
        .register()

    private val messagesSent: Counter = Counter.build(
        "th2_ldp_sse_events_count",
        "number of events sent into individual path"
    ).labelNames("uri").register()

    inline fun measureWrite(path: String, action: () -> Unit) {
        startTimer(path).use {
            action()
        }
    }

    fun startTimer(path: String): AutoCloseable = writingHistogram.labels(path).startTimer()

    fun messageSent(path: String, count: Int) {
        messagesSent.labels(path).inc(count.toDouble())
    }

    fun incConverted() {
        converted.inc()
    }
}