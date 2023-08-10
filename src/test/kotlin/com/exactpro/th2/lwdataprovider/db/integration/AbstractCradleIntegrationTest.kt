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

package com.exactpro.th2.lwdataprovider.db.integration

import com.datastax.oss.driver.api.core.CqlSession
import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.Direction
import com.exactpro.cradle.cassandra.CassandraCradleManager
import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings
import com.exactpro.cradle.messages.MessageToStore
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.util.DummyDataMeasurement
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractCradleIntegrationTest {
    protected lateinit var cradleStorage: CradleStorage
    protected lateinit var messageExtractor: CradleMessageExtractor

    @BeforeAll
    fun setUp() {
        val contactPoint = cassandraContainer.contactPoint

        val keyspace = "test_keyspace"
        CqlSession.builder()
            .addContactPoint(contactPoint)
            .withLocalDatacenter(cassandraContainer.localDatacenter)
            .build().use {
                it.execute("DROP KEYSPACE IF EXISTS $keyspace")
                it.execute("CREATE KEYSPACE $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
            }

        val cradleManager = CassandraCradleManager(
            CassandraConnectionSettings(
                contactPoint.hostName,
                contactPoint.port,
                cassandraContainer.localDatacenter,
            ),
            CassandraStorageSettings().apply {
                resultPageSize = 4
                this.keyspace = keyspace
            },
            true, // prepare storage
        )
        cradleStorage = cradleManager.storage
        messageExtractor = CradleMessageExtractor(
            cradleManager = cradleManager,
            DummyDataMeasurement,
        )
    }

    @AfterAll
    fun tearDown() {
        if (::cradleStorage.isInitialized) {
            cradleStorage.dispose()
        }
    }

    @Suppress("TestFunctionName")
    protected fun MessageToStore(
        bookId: BookId,
        sessionAlias: String,
        direction: Direction,
        sequence: Long,
        timestamp: Instant = Instant.now(),
        content: ByteArray = "hello".toByteArray(),
    ): MessageToStore {
        return MessageToStore.builder()
            .bookId(bookId)
            .sessionAlias(sessionAlias)
            .direction(direction)
            .sequence(sequence)
            .timestamp(timestamp)
            .content(content)
            .protocol("test")
            .build()
    }

    companion object {
        @Container
        private val cassandraContainer = CassandraContainer<Nothing>(DockerImageName.parse("cassandra:4.0.5"))
            .apply {
                withLogConsumer(Slf4jLogConsumer(LoggerFactory.getLogger("cassandra")))
            }
    }
}