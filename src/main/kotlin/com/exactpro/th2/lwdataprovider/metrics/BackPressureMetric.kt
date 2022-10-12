/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.lwdataprovider.metrics

import com.exactpro.th2.lwdataprovider.metrics.BackPressureMetric.Companion.BackPressureStatus.OFF
import com.exactpro.th2.lwdataprovider.metrics.BackPressureMetric.Companion.BackPressureStatus.ON
import io.prometheus.client.Counter
import io.prometheus.client.SimpleTimer.elapsedSecondsFromNanos
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class BackPressureMetric (
    requestId: String
) {
    private val lastTime = AtomicLong(DEFAULT_VALUE)
    private val lastStatus = AtomicReference(ON)
    private val metrics = EnumMap<BackPressureStatus, Counter.Child>(BackPressureStatus::class.java).apply {
        BackPressureStatus.values().forEach { status ->
            put(status, BACK_PRESSURE_COUNTER.labels(requestId, status.name))
        }
    }

    fun on() = collect(OFF)
    fun off() = collect(ON)

    private fun collect(status: BackPressureStatus) {
        if (lastStatus.getAndSet(status) != status) {
            val end = System.nanoTime()
            val start = lastTime.getAndSet(end)

            if (start != DEFAULT_VALUE) {
                requireNotNull(metrics[status]) {
                    "Metric for the $start status undeclared"
                }.inc(elapsedSecondsFromNanos(start, end))
            }
        }
    }

    companion object {
        private const val DEFAULT_VALUE = 0L
        private const val STATUS_LABEL: String = "status"

        private val BACK_PRESSURE_COUNTER: Counter = Counter.build()
            .name("th2_ldp_grpc_back_pressure_time_seconds")
            .help("Time is seconds which LwDP spent in both status of back pressure")
            .labelNames(REQUEST_ID_LABEL, STATUS_LABEL)
            .register()

        private enum class BackPressureStatus {
            ON,
            OFF,
        }
    }
}