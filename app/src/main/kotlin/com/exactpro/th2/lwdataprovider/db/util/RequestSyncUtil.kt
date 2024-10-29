/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.lwdataprovider.db.BaseDataSink
import io.github.oshai.kotlinlogging.KLogger
import java.time.Duration
import java.time.Instant

internal fun <F, D> getGenericWithSyncInterval(
    logger: KLogger,
    filters: List<F>,
    interval: Duration,
    sink: BaseDataSink,
    onData: (D) -> Unit,
    timeExtractor: (D) -> Instant,
    sourceSupplier: (F) -> Iterator<D>,
) {
    require(!interval.isZero && !interval.isNegative) { "incorrect sync interval $interval" }

    fun durationInRange(first: D, second: D): Boolean {
        return Duration.between(timeExtractor(first), timeExtractor(second)).abs() < interval
    }
    fun MutableList<D?>.minData(): D? = asSequence().filterNotNull().minByOrNull { timeExtractor(it) }

    val cradleIterators: List<Iterator<D>> = filters.map { sourceSupplier(it) }
    val lastTimestamps: MutableList<D?> = cradleIterators.mapTo(arrayListOf()) { if (it.hasNext()) it.next() else null }
    var minTimestampData: D? = lastTimestamps.minData() ?: run {
        logger.debug { "all cradle iterators are empty" }
        return
    }

    do {
        var allDataLoaded = cradleIterators.none { it.hasNext() }
        cradleIterators.forEachIndexed { index, cradleIterator ->
            sink.canceled?.apply {
                logger.info { "canceled the request: $message" }
                return
            }
            var lastMessage: D? = lastTimestamps[index]?.also { msg ->
                if (minTimestampData?.let { durationInRange(it, msg) } != false || allDataLoaded) {
                    onData(msg)
                    lastTimestamps[index] = null
                } else {
                    // messages is not in range for now
                    return@forEachIndexed
                }
            }
            var stop = false
            while (cradleIterator.hasNext() && !stop) {
                sink.canceled?.apply {
                    logger.info { "canceled the request: $message" }
                    return
                }
                val message: D = cradleIterator.next()
                if (minTimestampData?.let { durationInRange(it, message) } != false) {
                    onData(message)
                } else {
                    stop = true
                }
                lastMessage = message
            }
            if (cradleIterator.hasNext() || stop) {
                lastTimestamps[index] = lastMessage
                allDataLoaded = false
            }
        }
        minTimestampData = lastTimestamps.minData()
    } while (!allDataLoaded)
}