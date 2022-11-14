/*******************************************************************************
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.entities.exceptions.HandleDataException
import com.exactpro.th2.lwdataprovider.handlers.AbstractCancelableHandler
import java.util.concurrent.BlockingQueue

class GrpcHandler<IN>(
    private val buffer: BlockingQueue<GrpcEvent>,
    private val transform: (IN) -> GrpcEvent,
) : AbstractCancelableHandler<IN>() {

    override fun complete() {
        if (!isAlive) return
        buffer.put(GrpcEvent(close = true))
    }

    override fun writeErrorMessage(text: String) {
        writeErrorMessage(HandleDataException(text))
    }

    override fun writeErrorMessage(error: Throwable) {
        if (!isAlive) return
        buffer.put(GrpcEvent(error = error))
    }

    override fun handleNext(data: IN) {
        if (!isAlive) return
        buffer.put(transform.invoke(data))
    }

}