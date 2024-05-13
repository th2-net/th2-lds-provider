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

interface GenericDataSink<T> : BaseDataSink {
    fun onNext(data: T)
}
interface EventDataSink<T> : GenericDataSink<T> {
    fun onNext(listData: Collection<T>): Unit = listData.forEach(::onNext)
}

interface MessageDataSink<M, T> : BaseDataSink {
    fun onNext(marker: M, data: T)
    fun onNext(marker: M, listData: Collection<T>): Unit = listData.forEach { onNext(marker, it) }
}

interface BaseDataSink : AutoCloseable {
    val canceled: CancellationReason?
    fun onError(message: String, id: String? = null, batchId: String? = null)
    fun onError(ex: Exception, id: String? = null, batchId: String? = null): Unit =
        onError(ex.message ?: ex.toString(), id, batchId)
    fun completed()
    override fun close() = completed()
}

data class CancellationReason(val message: String)

