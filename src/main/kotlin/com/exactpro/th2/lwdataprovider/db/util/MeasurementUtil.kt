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

package com.exactpro.th2.lwdataprovider.db.util

import com.exactpro.th2.lwdataprovider.db.DataMeasurement

fun <T> Iterator<T>.asIterableWithMeasurements(type: String, dataMeasurement: DataMeasurement): Iterable<T> =
    Iterable { MeasurementIterator(this, type, dataMeasurement) }

fun <T> Iterator<T>.withMeasurements(type: String, dataMeasurement: DataMeasurement): Iterator<T> =
    MeasurementIterator(this, type, dataMeasurement)

private class MeasurementIterator<T>(
    private val original: Iterator<T>,
    type: String,
    private val dataMeasurement: DataMeasurement,
) : Iterator<T> by original {
    private val actionName: String = "${type}_next_request"
    override fun hasNext(): Boolean = dataMeasurement.start(actionName).use {
        original.hasNext()
    }
}