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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.BookInfo
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.PageInfo
import mu.KotlinLogging
import java.time.Instant

class GeneralCradleExtractor(
    cradleManager: CradleManager,
) {
    private val storage: CradleStorage = cradleManager.storage

    fun getBookIDs(): Set<BookId> = storage.listBooks().mapTo(hashSetOf()) { BookId(it.name) }

    //FIXME: use another cradle API to get pages by book id and time interval
    fun getPageInfos(bookId: BookId, from: Instant, to: Instant, sink: GenericDataSink<PageInfo>) {
        require(!to.isBefore(from)) { "from ($from) must be <= to ($to)" }
        //        WHERE T.start <= R.end AND T.end >= Q.start
        val predicate: (PageInfo) -> Boolean = {
                pageInfo -> pageInfo.started <= to && ( pageInfo.ended == null || pageInfo.ended >= from)
        }
        storage.getAllPages(bookId)
            .asSequence()
            .filter { pageInfo -> pageInfo.started != null }
            .dropWhile { pageInfo -> !predicate.invoke(pageInfo) }
            .takeWhile(predicate)
            .forEach { pageInfo ->
                sink.onNext(pageInfo)
                sink.canceled?.apply {
                    LOGGER.info { "page info processing canceled: $message" }
                    return
                }
            }
    }

    fun getCachedBooks(): Collection<BookInfo> = storage.books

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}