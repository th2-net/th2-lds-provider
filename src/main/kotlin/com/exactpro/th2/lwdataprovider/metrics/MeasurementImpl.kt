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

import com.exactpro.th2.lwdataprovider.db.Measurement
import mu.KotlinLogging

internal class MeasurementImpl(
    private val name: String,
    private val timer: AutoCloseable
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
        timer.close()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}