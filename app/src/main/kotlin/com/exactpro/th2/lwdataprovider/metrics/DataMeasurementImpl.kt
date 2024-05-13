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

import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.db.Measurement
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.SimpleTimer

class DataMeasurementImpl private constructor(
    name: String,
    registry: CollectorRegistry,
) : DataMeasurement {

    private val stepMetricsCounter = Counter.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time_count", "Number of measurement on each action for $name"
    ).labelNames("action")
        .register(registry)

    private val stepMetricsSum = Counter.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time_sum", "Sum time of measurement on each action for $name"
    ).labelNames("action")
        .register(registry)

    override fun start(name: String): Measurement {
        return Timer(stepMetricsCounter.labels(name), stepMetricsSum.labels(name))
    }

    companion object {

        @JvmStatic
        fun create(registry: CollectorRegistry, name: String): DataMeasurement =
            DataMeasurementImpl(name, registry)

        private class Timer(
            private val counter: Counter.Child,
            private val sum: Counter.Child
        ) : Measurement {

            private val start = System.nanoTime()
            override fun stop() {
                counter.inc()
                sum.inc(SimpleTimer.elapsedSecondsFromNanos(start, System.nanoTime()))
            }

        }
    }
}

