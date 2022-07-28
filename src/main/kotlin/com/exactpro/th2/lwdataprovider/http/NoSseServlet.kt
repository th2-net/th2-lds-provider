/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.NoSseResponseWriter
import com.exactpro.th2.lwdataprovider.SseEvent
import org.eclipse.jetty.http.HttpHeader
import org.eclipse.jetty.http.HttpStatus
import java.util.concurrent.BlockingQueue
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

open class NoSseServlet : HttpServlet() {
    
    protected open fun waitAndWrite(queue: BlockingQueue<SseEvent>, resp: HttpServletResponse) {
        resp.contentType = "application/json"
        resp.addHeader(HttpHeader.CACHE_CONTROL.asString(), "no-cache, no-store")

        val writer = NoSseResponseWriter(resp.writer)
        val event = queue.take()
        resp.status = statusFromEventType(event.event)
        writer.writeEvent(event)
        writer.closeWriter()    
    }

    private fun statusFromEventType(event: EventType): Int {
        return if (event == EventType.ERROR) {
            HttpStatus.NOT_FOUND_404
        } else {
            HttpStatus.OK_200
        }
    }

    protected fun getParameters(req: HttpServletRequest): Map<String, List<String>> {
        return req.parameterMap.mapValues { it.value.toList() }
    }

}