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

import org.eclipse.jetty.http.HttpHeader
import org.eclipse.jetty.http.HttpStatus
import java.util.*
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetTestSpeedServlet2() : HttpServlet() {
    
    private var str : String;


    init {
        val r = StringBuilder();
        val alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".toCharArray()
        val random = Random()
        for (i in 1..1024*1024)
            r.append(alphabet[random.nextInt(alphabet.size)])
        str = r.toString()
    }

    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {

        var parameter = req!!.getParameter("flush")
        var flush = parameter != null && parameter.isNotEmpty() && "true" == parameter
        
        resp!!.contentType = "text/event-stream"
        resp.status = HttpStatus.OK_200
        resp.addHeader(HttpHeader.CACHE_CONTROL.asString(), "no-cache, no-store")
        resp.writer.use { 
            for (i in 1..1024) {
                it.print(str)
                if (flush)
                    it.flush()    
            }
        }
        
    }
}