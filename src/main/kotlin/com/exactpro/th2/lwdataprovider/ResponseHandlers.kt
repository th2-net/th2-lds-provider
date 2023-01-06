/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.PageInfoResponse
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.function.Supplier

interface ResponseHandler<T> : BasicResponseHandler {
    fun handleNext(data: T)
}

interface BasicResponseHandler {
    val isAlive: Boolean
    fun complete()
    fun writeErrorMessage(text: String, id: String? = null, batchId: String? = null)
    fun writeErrorMessage(error: Throwable, id: String? = null, batchId: String? = null)
        = writeErrorMessage(ExceptionUtils.getMessage(error), id, batchId)
}

interface CancelableResponseHandler : BasicResponseHandler {
    fun cancel()
}

interface KeepAliveListener {
    fun update()
    val lastTimestampMillis: Long
}

data class GrpcEvent(val message: Supplier<MessageSearchResponse>? = null, val event: EventResponse? = null, val error: Throwable? = null, val close: Boolean = false,
                     val pageInfo: PageInfoResponse? = null)