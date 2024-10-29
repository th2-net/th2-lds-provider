/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.db.GeneralCradleExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.AllPageInfoRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SsePageInfosSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.PageId
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.Executor
import com.exactpro.cradle.PageId as CradlePageId
import com.exactpro.cradle.PageInfo as CradlePageInfo

class GeneralCradleHandler(
    private val extractor: GeneralCradleExtractor,
    private val executor: Executor,
) {
    fun getBookIDs(): Set<BookId> = extractor.getBookIDs()

    fun getPageInfos(
        request: SsePageInfosSearchRequest,
        handler: ResponseHandler<PageInfo>,
    ) {
        executor.execute {
            K_LOGGER.info { "Starting loading pages for $request" }
            GenericDataSink(
                handler,
                limit = request.resultCountLimit,
                CradlePageInfo::convert,
            ).use { sink ->
                try {
                    extractor.getPageInfos(
                        request.bookId,
                        request.startTimestamp,
                        request.endTimestamp,
                        sink,
                    )
                } catch (ex: Exception) {
                    K_LOGGER.error(ex) { "error during getting pages for request $request" }
                    sink.onError(ex)
                }
            }
            K_LOGGER.info { "All pages loaded for request $request" }
        }
    }

    fun getAllPageInfos(
        request: AllPageInfoRequest,
        handler: ResponseHandler<PageInfo>,
    ) {
        executor.execute {
            K_LOGGER.info { "Starting loading pages for $request" }
            GenericDataSink(
                handler,
                limit = request.limit,
                CradlePageInfo::convert,
            ).use { sink ->
                try {
                    extractor.getAllPageInfos(request.bookId, sink)
                } catch (ex: Exception) {
                    K_LOGGER.error(ex) { "error during getting all pages for request $request" }
                    sink.onError(ex)
                }
            }
            K_LOGGER.info { "All pages loaded for request $request" }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
    }
}

private fun CradlePageInfo.convert() = PageInfo(
    id.convert(),
    comment,
    started,
    ended,
    updated,
    removed
)

private fun CradlePageId.convert() = PageId(bookId.name, name)