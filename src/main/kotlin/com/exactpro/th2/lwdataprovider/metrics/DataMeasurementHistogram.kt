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
import io.prometheus.client.Histogram

class DataMeasurementHistogram private constructor(
    name: String,
    registry: CollectorRegistry,
    buckets: DoubleArray,
) : DataMeasurement {
    private val stepMetrics = Histogram.build(
        "th2_ldp_${name.replace(' ', '_').lowercase()}_time", "Time spent on each action for $name"
    ).buckets(*(buckets.takeIf { it.isNotEmpty() } ?: DEFAULT_BUCKETS))
        .labelNames("action")
        .register(registry)

    override fun start(name: String): Measurement {
        return MeasurementImpl(name, stepMetrics.labels(name).startTimer())
    }

    companion object {
        private val DEFAULT_BUCKETS =
            doubleArrayOf(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)

        @JvmStatic
        fun create(registry: CollectorRegistry, name: String, vararg buckets: Double): DataMeasurement =
            DataMeasurementHistogram(name, registry, buckets)
    }
}

