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

import io.prometheus.client.Gauge

object ResponseQueue {
    private val responseQueueSize: Gauge = Gauge.build(
        "th2_ldp_response_queue_size",
        "the current size of the response queue"
    ).labelNames("uri").register()

    fun currentSize(path: String, size: Int) {
        responseQueueSize.labels(path).set(size.toDouble())
    }
}