/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.eclipse.jetty.http.HttpStatus
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetSessionAliases(
    private val messagesHandler: SearchMessagesHandler,
    private val jsonMapper: ObjectMapper,
) : NoSseServlet() {
    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        LOGGER.info { "Extracting session aliases" }
        resp.writeHeaders()

        val result: Any = try {
            messagesHandler.extractStreamNames()
            resp.status = HttpStatus.OK_200
        } catch (ex: Exception) {
            LOGGER.error(ex) { "Cannot get session aliases" }
            resp.status = HttpStatus.INTERNAL_SERVER_ERROR_500
            ExceptionInfo(ex.javaClass.name, ExceptionUtils.getRootCauseMessage(ex))
        }

        resp.writer.use {
            it.write(jsonMapper.writeValueAsString(result))
            it.flush()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}