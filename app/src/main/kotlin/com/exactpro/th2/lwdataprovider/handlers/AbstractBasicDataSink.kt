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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.th2.lwdataprovider.BasicResponseHandler
import com.exactpro.th2.lwdataprovider.db.BaseDataSink
import com.exactpro.th2.lwdataprovider.db.CancellationReason

abstract class AbstractBasicDataSink(
    protected open val handler: BasicResponseHandler,
    val limit: Int? = null,
) : BaseDataSink {
    init {
        if (limit != null) {
            require(limit > 0) { "limit must be a positive number but was $limit" }
        }
    }
    private var error: String? = null
    var loadedData: Long = 0
        protected set
    override val canceled: CancellationReason?
        get() = when {
            limit != null && limit in 1..loadedData -> CancellationReason("limit $limit has been reached")
            !handler.isAlive -> CancellationReason("request was canceled")
            else -> error?.let { CancellationReason("error $it") }
        }

    override fun onError(ex: Exception, id: String?, batchId: String?) {
        onError(ex.message ?: ex.toString(), id, batchId)
    }

    override fun onError(message: String, id: String?, batchId: String?) {
        error = message
        handler.writeErrorMessage(message, id, batchId)
        handler.complete()
    }
}