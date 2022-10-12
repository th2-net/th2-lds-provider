/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
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

import io.prometheus.client.Gauge
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

class MetricBlockingQueue<T> (
    private val gauge: Gauge,
    private val origin: BlockingQueue<T>,
    private val capacity: Int? = null
) : BlockingQueue<T> by origin {
    
        override fun add(element: T): Boolean = origin.add(element).also { if (it) { gauge.inc() } }

        override fun offer(e: T): Boolean = origin.offer(e).also { if (it) { gauge.inc() } }

        @Throws(InterruptedException::class)
        override fun put(e: T) = origin.put(e).also { gauge.inc() }

        @Throws(InterruptedException::class)
        override fun offer(e: T, timeout: Long, unit: TimeUnit): Boolean = origin.offer(e, timeout, unit).also { if (it) { gauge.inc() } }

        @Throws(InterruptedException::class)
        override fun take(): T = origin.take().also { gauge.dec() }

        @Throws(InterruptedException::class)
        override fun poll(timeout: Long, unit: TimeUnit): T? = origin.poll(timeout, unit).also { if (it != null) { gauge.dec() } }

        override fun remainingCapacity(): Int = origin.remainingCapacity().also { remaining -> capacity?.let { gauge.set((capacity - remaining).toDouble()) } }

        override fun remove(element: T): Boolean = origin.remove(element).also { if (it) { gauge.dec() } }

        override fun drainTo(c: MutableCollection<in T>): Int = origin.drainTo(c).also { gauge.dec(it.toDouble()) }

        override fun drainTo(c: MutableCollection<in T>, maxElements: Int): Int = origin.drainTo(c, maxElements).also { gauge.dec(it.toDouble()) }
}