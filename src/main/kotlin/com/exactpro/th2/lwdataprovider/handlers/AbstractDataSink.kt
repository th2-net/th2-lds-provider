/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.db.EventDataSink
import com.exactpro.th2.lwdataprovider.db.MessageDataSink

open class GenericDataSink<IN, OUT>(
    override val handler: ResponseHandler<OUT>,
    limit: Int? = null,
    private val convert: (IN) -> OUT,
) : AbstractBasicDataSink(handler, limit), EventDataSink<IN> {
    override fun completed() {
        handler.complete()
    }

    override fun onNext(data: IN) {
        if (limit == null || loadedData < limit) {
            loadedData++
            handler.handleNext(convert(data))
        }
    }
}

class SingleTypeDataSink<T>(
    handler: ResponseHandler<T>,
    limit: Int? = null,
) : GenericDataSink<T, T>(handler, limit, { it })

abstract class AbstractMessageDataSink<M, T, H : BasicResponseHandler>(
    override val handler: H,
    limit: Int? = null,
) : AbstractBasicDataSink(handler, limit), MessageDataSink<M, T>