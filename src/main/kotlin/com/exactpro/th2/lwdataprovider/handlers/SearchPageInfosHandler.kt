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

import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.entities.requests.SsePageInfosSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import mu.KotlinLogging
import java.util.concurrent.Executor

class SearchPageInfosHandler(
    private val cradle: GeneralCradleHandler,
    private val threadPool: Executor,
) {

    fun loadPageInfos(request: SsePageInfosSearchRequest, requestContext: ResponseHandler<PageInfo>) {
        threadPool.execute {
            ResponseHandlerDataSink(requestContext, request.resultCountLimit).use {
                try {
                    cradle.getPageInfos(request, it)
                } catch (e: Exception) {
                    K_LOGGER.error(e) { "error during loading page infos $request" }
                    it.onError(e)
                }
            }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
    }
}