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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookListEntry
import com.exactpro.th2.lwdataprovider.db.GeneralCradleExtractor
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isEqualTo
import strikt.jackson.isArray
import strikt.jackson.textValues

internal class TestGetBookIDs : AbstractHttpHandlerTest<GetBookIDs>() {
    override fun createHandler(): GetBookIDs {
        return GetBookIDs(GeneralCradleHandler(GeneralCradleExtractor(manager)))
    }

    @Test
    fun `returns books`() {
        doReturn(
            listOf(
                "book1", "book2", "book3"
            ).map { BookListEntry(it, "v1") }
        ).whenever(storage).listBooks()

        startTest { _, client ->
            expectThat(client.get("/books")) {
                get { code } isEqualTo HttpStatus.OK.code
                jsonBody()
                    .isArray()
                    .textValues()
                    .containsExactlyInAnyOrder("book1", "book2", "book3")
            }
        }
    }
}