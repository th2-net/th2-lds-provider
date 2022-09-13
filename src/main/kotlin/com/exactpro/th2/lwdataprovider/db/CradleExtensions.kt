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
package com.exactpro.th2.lwdataprovider.db

fun <T> Iterable<T>.toMetricIterable(inc: () -> Unit): Iterable<T> = MetricIterable(inc, this)

private class MetricIterable<T>(
    private val inc: () -> Unit,
    private val originalIterable: Iterable<T>,
) : Iterable<T> {
    override fun iterator(): Iterator<T> = MetricIterator(inc, originalIterable.iterator())
}

private class MetricIterator<T>(
    private val inc: () -> Unit,
    private val originalIterator: Iterator<T>
): Iterator<T> {
    override fun hasNext(): Boolean = originalIterator.hasNext()

    override fun next(): T = originalIterator.next().also {
        inc.invoke()
    }
}
