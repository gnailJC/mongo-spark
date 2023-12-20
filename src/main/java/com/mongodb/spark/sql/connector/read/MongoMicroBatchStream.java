/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.spark.sql.connector.read;

import static com.mongodb.spark.sql.connector.read.MongoInputPartitionHelper.generateMicroBatchPartitions;
import static java.lang.String.format;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.schema.BsonDocumentToRowConverter;
import com.mongodb.spark.sql.connector.schema.InferSchema;
import java.time.Instant;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoMicroBatchStream defines how to read a stream of data from MongoDB.
 *
 * <p>Utilizes MongoDBs change stream functionality, the continuous streams will consist of <a
 * href="https://docs.mongodb.com/manual/reference/change-events/">change events</a>.
 *
 * <p>Note: Requires MongoDB 4.2+ To support continuing a change stream after a collection has been
 * dropped.
 *
 * <p>Uses seconds since epoch offsets to create boundaries between partitions.
 */
final class MongoMicroBatchStream implements MicroBatchStream, SupportsTriggerAvailableNow {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoMicroBatchStream.class);
  private final StructType schema;
  private final MongoOffsetStore mongoOffsetStore;
  private final ReadConfig readConfig;
  private final BsonDocumentToRowConverter bsonDocumentToRowConverter;
  private volatile Long lastTime = Instant.now().getEpochSecond();

  /**
   * Construct a new instance
   *
   * @param schema the schema for the data
   * @param readConfig the read configuration
   */
  MongoMicroBatchStream(
      final StructType schema, final String checkpointLocation, final ReadConfig readConfig) {
    Assertions.validateConfig(
        schema,
        (s) -> !s.isEmpty()
            && (!InferSchema.isInferred(s) || readConfig.streamPublishFullDocumentOnly()),
        () ->
            "Mongo micro batch streams require a schema to be explicitly defined, unless using publish full document only.");
    this.schema = schema;
    this.mongoOffsetStore = new MongoOffsetStore(
        SparkContext.getOrCreate().hadoopConfiguration(),
        checkpointLocation,
        MongoOffset.getInitialOffset(readConfig));
    this.readConfig = readConfig;
    this.bsonDocumentToRowConverter =
        new BsonDocumentToRowConverter(schema, readConfig.outputExtendedJson(), readConfig.ignoreInvalidFieldData());
  }

  @Override
  public Offset latestOffset() {
    throw new UnsupportedOperationException(
            "That latestOffset(Offset, ReadLimit) method should be called instead of this method.");
  }

  @Override
  public InputPartition[] planInputPartitions(final Offset start, final Offset end) {
    return generateMicroBatchPartitions(
        schema, readConfig, (BsonTimestampOffset) start, (BsonTimestampOffset) end);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new MongoMicroBatchPartitionReaderFactory(bsonDocumentToRowConverter, readConfig);
  }

  @Override
  public Offset initialOffset() {
    return mongoOffsetStore.initialOffset();
  }

  @Override
  public Offset deserializeOffset(final String json) {
    return MongoOffset.fromJson(json);
  }

  @Override
  public void commit(final Offset end) {
    LOGGER.info("MicroBatchStream commit: {}", end);
    mongoOffsetStore.updateOffset((MongoOffset) end);
  }

  @Override
  public void stop() {
    LOGGER.info("MicroBatchStream stopped.");
  }

  @Override
  public Offset latestOffset(final Offset startOffset, final ReadLimit limit) {
    LOGGER.info(format("StartOffset is `%s`, ReadLimit is `%s`", startOffset, limit));
    Assertions.ensureArgument(() -> startOffset instanceof BsonTimestampOffset, () -> "StartOffset must be instance of BsonTimestampOffset");
    Assertions.ensureArgument(() -> limit instanceof MongoMicroBatchTimeDurationLimit, () -> "ReadLimit must be instance of MongoMicroBatchTimeDurationLimit");
    MongoMicroBatchTimeDurationLimit timeDurationLimit = (MongoMicroBatchTimeDurationLimit) limit;
    long now = Instant.now().getEpochSecond();
    if (lastTime < now) {
      lastTime = now;
    }
    if (timeDurationLimit.isEnableTimeDurationLimit()) {
      BsonDocument startOffsetDoc = BsonDocument.parse(startOffset.json());
      BsonValue offset = startOffsetDoc.get("offset");
      if (offset.isTimestamp()) {
        int initialTime = offset.asTimestamp().getTime();
        long timeDuration = timeDurationLimit.timeDuration();
        long durationLastTime = initialTime + timeDuration;
        durationLastTime = Math.min(durationLastTime, now);
        LOGGER.info(format(
                "Change lastTime from `%s` to `%s`. initialOffset is `%s` ,'time.duration' is `%s` and 'now' is `%s`",
                lastTime, durationLastTime, initialTime, timeDuration, now
        ));
        lastTime = durationLastTime;
      } else {
        throw new MongoSparkException(format("Invalid offset expected a timestamp  token: `%s`.", offset));
      }
    }
    LOGGER.info(format("LatestOffset: %s", lastTime.intValue()));
    return new BsonTimestampOffset(new BsonTimestamp(lastTime.intValue(), 0));
  }

  @Override
  public void prepareForTriggerAvailableNow() {
    long now = Instant.now().getEpochSecond();
    if (lastTime < now) {
      lastTime = now;
    }
  }

  @Override
  public ReadLimit getDefaultReadLimit() {
    return new MongoMicroBatchTimeDurationLimit(readConfig.getStreamMicroBatchTimeDuration());
  }
}
