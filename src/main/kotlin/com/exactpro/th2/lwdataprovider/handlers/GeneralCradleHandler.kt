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

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.db.EventDataSink
import com.exactpro.th2.lwdataprovider.db.GeneralCradleExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.SsePageInfosSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.PageId
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import mu.KotlinLogging

typealias CradlePageInfo = com.exactpro.cradle.PageInfo
typealias CradlePageId = com.exactpro.cradle.PageId

class GeneralCradleHandler(
    private val extractor: GeneralCradleExtractor,
) {
    fun getBookIDs(): Set<BookId> = extractor.getBookIDs()

    fun getPageInfos(
        request: SsePageInfosSearchRequest,
        sink: EventDataSink<PageInfo>
    ) {
//        WHERE T.start <= R.end AND T.end >= Q.start
        val predicate: (CradlePageInfo) -> Boolean = {
                pageInfo -> pageInfo.started <= request.endTimestamp && pageInfo.ended >= request.startTimestamp
        }
        extractor.getPageInfos(request.bookId)
            .asSequence()
            .filter { pageInfo -> pageInfo.ended != null && pageInfo.started != null }
            .dropWhile { pageInfo -> !predicate.invoke(pageInfo) }
            .takeWhile(predicate)
            .forEach { pageInfo ->
                sink.onNext(pageInfo.convert())
                sink.canceled?.apply {
                    K_LOGGER.info { "page info processing canceled: $message" }
                    return
                }
            }

        sink.canceled?.apply {
            K_LOGGER.info { "Loading pages stopped: $message" }
            return
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private fun CradlePageInfo.convert() = PageInfo(
            id.convert(),
            comment,
            started,
            ended,
            updated,
            removed
        )

        private fun CradlePageId.convert() = PageId(bookId.name, name)
    }
}