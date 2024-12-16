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

package com.exactpro.th2.lwdataprovider.entities.responses

import io.javalin.openapi.OpenApiRequired
import java.time.Instant

data class PageInfo(
    val id: PageId,
    @get:OpenApiRequired
    val comment: String?,

    val started: Instant,
    @get:OpenApiRequired
    val ended: Instant?,

    @get:OpenApiRequired
    val updated: Instant?,
    @get:OpenApiRequired
    val removed: Instant?
)

data class PageId(
    val book: String,
    val name: String,
)