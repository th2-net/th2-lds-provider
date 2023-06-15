package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.messages.StoredMessageId
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import kotlin.system.measureTimeMillis

internal class TestStoredMessageIdSerializer {
    @ParameterizedTest
    @ValueSource(strings = ["simple", "with\\:escape"])
    fun `returns the same value as stored message id for book and alias`(value: String) {
        val expectedString = "$value:$value:1:20220101010101000000001:1686312182591833810"
        val id = StoredMessageId.fromString(expectedString)
        val result = Json.encodeToString(StoredMessageIdSerializer, id)
        expectThat(result).isEqualTo(Json.encodeToString(String.serializer(), id.toString()))
    }

    @ParameterizedTest
    @ValueSource(strings = ["20220101010101000000001", "20221231235959999999999"])
    fun `returns the same value as stored message id for timestamp`(value: String) {
        val expectedString = "book:alias:1:$value:1686312182591833810"
        val id = StoredMessageId.fromString(expectedString)
        val result = Json.encodeToString(StoredMessageIdSerializer, id)
        expectThat(result).isEqualTo(Json.encodeToString(String.serializer(), id.toString()))
    }
}