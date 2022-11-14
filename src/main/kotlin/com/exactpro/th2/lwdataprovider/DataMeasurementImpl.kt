/*
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
 */

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.db.Measurement
import io.prometheus.client.Histogram
import mu.KotlinLogging


object DataMeasurementImpl : DataMeasurement {
    override fun start(name: String): Measurement {
        return MeasurementImpl(name, STEP_METRICS.labels(name).startTimer())
    }

    private val STEP_METRICS = Histogram.build(
        "th2_ldp_message_pipeline_hist_time", "Time spent on each step for a message"
    ).buckets(.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 75.0)
        .labelNames("step")
        .register()
}

private class MeasurementImpl(
    private val name: String,
    private val timer: Histogram.Timer
) : Measurement {
    init {
        LOGGER.trace { "Step $name started with timer ${timer.hashCode()}" }
    }

    private var finished: Boolean = false
    override fun stop() {
        if (finished) {
            return
        }
        LOGGER.trace { "Step $name finished with timer ${timer.hashCode()}" }
        finished = true
        timer.observeDuration()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}