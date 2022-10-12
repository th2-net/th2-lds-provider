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