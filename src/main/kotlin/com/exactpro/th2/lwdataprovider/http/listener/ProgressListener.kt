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

package com.exactpro.th2.lwdataprovider.http.listener

import com.exactpro.th2.lwdataprovider.SseEvent

interface ProgressListener {
    fun onStart()
    fun onError(ex: Exception)
    fun onError(errorEvent: SseEvent.ErrorData)
    fun onCompleted()

    fun onCanceled()
}

@JvmField
val DEFAULT_PROCESS_LISTENER: ProgressListener = object : ProgressListener {
    override fun onStart() = Unit

    override fun onError(ex: Exception) = Unit

    override fun onError(errorEvent: SseEvent.ErrorData) = Unit

    override fun onCompleted() = Unit

    override fun onCanceled() = Unit
}