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

interface DataSink<T> : AutoCloseable {
    val canceled: CancellationReason?
    fun onNext(data: T)
    fun onNext(listData: Collection<T>): Unit = listData.forEach(::onNext)
    fun onError(message: String)
    fun onError(ex: Exception): Unit = onError(ex.message ?: ex.toString())
    fun completed()
    override fun close() = completed()
}

data class CancellationReason(val message: String)

