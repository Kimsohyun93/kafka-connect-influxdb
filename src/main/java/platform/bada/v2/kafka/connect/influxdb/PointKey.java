package platform.bada.v2.kafka.connect.influxdb;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

class PointKey implements Comparable<PointKey> {
  public final String measurement;
  public final long time;
  public final Map<String, String> tags;


  private PointKey(String measurement, long time, Map<String, String> tags) {
    this.measurement = measurement;
    this.tags = ImmutableMap.copyOf(tags);
    this.time = time;
  }

  public static PointKey of(String measurement, long time, Map<String, String> tags) {
    return new PointKey(measurement, time, tags);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("measurement", this.measurement)
            .add("time", this.time)
            .add("tags", this.tags)
            .toString();
  }

  @Override
  public int compareTo(PointKey that) {
    return ComparisonChain.start()
            .compare(this.measurement, that.measurement)
            .compare(this.time, that.time)
            .compare(Objects.hashCode(this.tags), Objects.hashCode(that.tags))
            .result();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.measurement, this.time, this.tags);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PointKey) {
      return 0 == compareTo((PointKey) obj);
    } else {
      return false;
    }
  }
}