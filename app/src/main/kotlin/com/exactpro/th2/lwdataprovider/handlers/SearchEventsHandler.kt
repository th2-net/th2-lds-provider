/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.db.CradleEventExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executor

private val logger = KotlinLogging.logger { }

class SearchEventsHandler(
    private val cradle: CradleEventExtractor,
    private val threadPool: Executor,
) {

    fun loadAllScopes(bookId: BookId): Set<String> = cradle.getAllEventsScopes(bookId)

    fun loadScopes(bookId: BookId, start: Instant, end: Instant): Iterator<String> =
        cradle.getScopes(bookId, start, end)

    fun loadEvents(request: SseEventSearchRequest, requestContext: ResponseHandler<Event>) {
        threadPool.execute {
            SingleTypeDataSink(requestContext, request.resultCountLimit).use {
                try {
                    cradle.getEvents(request, it)
                } catch (e: Exception) {
                    logger.error(e) { "error during loading events $request" }
                    it.onError(e)
                }
            }
        }
    }

    fun loadOneEvent(request: GetEventRequest, requestContext: ResponseHandler<Event>) {

        threadPool.execute {
            SingleTypeDataSink(requestContext, limit = null).use {
                try {
                    cradle.getSingleEvents(request, it)
                } catch (e: Exception) {
                    logger.error(e) { "error during loading singe event $request" }
                    it.onError(e, request.eventId, request.batchId)
                }
            }
        }
    }
}