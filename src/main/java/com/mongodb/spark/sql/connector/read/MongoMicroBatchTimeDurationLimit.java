package com.mongodb.spark.sql.connector.read;

import org.apache.spark.sql.connector.read.streaming.ReadLimit;

public class MongoMicroBatchTimeDurationLimit implements ReadLimit {
  private final long timeDuration;

  MongoMicroBatchTimeDurationLimit(long timeDuration) {
    this.timeDuration = timeDuration;
  }

  public long timeDuration() {
    return this.timeDuration;
  }

  public boolean isEnableTimeDurationLimit() {
    return timeDuration > 0;
  }

  @Override
  public String toString() {
    return "TimeDuration: " + timeDuration();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MongoMicroBatchTimeDurationLimit other = (MongoMicroBatchTimeDurationLimit) o;
    return other.timeDuration() == timeDuration();
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.timeDuration);
  }
}
