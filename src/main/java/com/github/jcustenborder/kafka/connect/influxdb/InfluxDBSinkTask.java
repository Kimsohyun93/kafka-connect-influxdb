/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.influxdb;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
//import com.google.common.base.Joiner;
import com.google.common.base.Strings;
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSet;
//import org.apache.kafka.connect.data.Decimal;
//import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
//import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
// import java.util.Set;
import java.util.ArrayList;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class InfluxDBSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(InfluxDBSinkTask.class);
  InfluxDBSinkConnectorConfig config;
  InfluxDBFactory factory = new InfluxDBFactoryImpl();
  InfluxDB influxDB;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = new InfluxDBSinkConnectorConfig(settings);
    this.influxDB = this.factory.create(this.config);
  }

  static final Schema TAG_OPTIONAL_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
  @Override
  public void put(Collection<SinkRecord> records) {
    /**
     * Sink Records Format
     *
     * [
     *   SinkRecord{
     *     kafkaOffset=0,
     *     timestampType=CreateTime
     *   }
     *   ConnectRecord{
     *     topic='influx-test3',
     *     kafkaPartition=0,
     *     key=null,
     *     keySchema=null,
     *     value=Struct{
     *       measurement=kafka_ae,
     *       tags=kafka_cnt,
     *       fields={
     *         "TAG_ID": 8065243138,
     *         "POS_TYPE": 1,
     *         "POS_TIME": "2021-10-14 08:53:30.096",
     *         "XPOS": 42.7068856917,
     *         "YPOS": 2.3179656097
     *
     *       }
     *     },
     *     valueSchema=Schema{
     *       STRUCT
     *     },
     *     timestamp=1647393968223,
     *     headers=ConnectHeaders(headers=)
     *   },
     *   SinkRecord{
     *     kafkaOffset=0,
     *     ...
     * ]
     */
    if (null == records || records.isEmpty()) {
      return;
    }
    JSONParser jParser = new JSONParser();
    Map<PointKey, Map<String, Object>> builders = new HashMap<>(records.size());

    System.out.println("**************** \n \n \n \n \n \n****************** \n \n \n \n HERE \n **************** \n");
    System.out.println("THIS IS VALUE OF RECORDS : " + String.valueOf(records));
    for (SinkRecord record : records) {
      System.out.println("THIS IS VALUE OF String : " + record.value().toString());
      Long kafkaOffset = record.kafkaOffset();
      JSONObject jcon = null;
      try {
        jcon = (JSONObject) jParser.parse(record.value().toString());
      } catch (ParseException e) {
        e.printStackTrace();
      }
      System.out.println("THIS IS VALUE OF JSONObject : " + jcon);

      String measurement = (String) jcon.get("measurement");
      System.out.println("THIS IS VALUE OF MEASUREMENT : " + measurement);
      if (Strings.isNullOrEmpty(measurement.toString())) {
        throw new DataException("measurement is a required field.");
      }

      JSONObject tagField = null;
      final Map<String, String> tags = new HashMap<String, String>();
      try {
        tagField = (JSONObject) jParser.parse(jcon.get("tags").toString());
        ArrayList<String> tagKeys = new ArrayList<String>(tagField.keySet());
        for (String tagKey : tagKeys) {
          System.out.println("THIS IS VALUE OF TAG : " + tagKey + " | " + tagField.get(tagKey).toString());
          tags.put(tagKey, tagField.get(tagKey).toString());
        }
        System.out.println("THIS IS VALUE OF CONTAINER : " + tags.toString());
      } catch (ParseException e) {
        e.printStackTrace();
      }
//      String tag = (String) tagField.get("container");

      final long time = record.timestamp();
      PointKey key = PointKey.of(measurement, time, tags);
      Map<String, Object> fields = builders.computeIfAbsent(key, pointKey -> new HashMap<>(100));

      JSONObject dataField = null;
      try {
        dataField = (JSONObject) jParser.parse(jcon.get("fields").toString());
        System.out.println("THIS IS VALUE OF Data Fields : " + dataField);
        ArrayList<String> fieldKeys = new ArrayList<String>(dataField.keySet());
        System.out.println("THIS IS VALUE OF Data Fields KEY SET : " + dataField.keySet());

        for (String fieldKey : fieldKeys) {
          System.out.println("THIS IS VALUE OF Data Fields KEYs : " + fieldKey);
          fields.put(fieldKey, dataField.get(fieldKey));
        }
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    BatchPoints.Builder batchBuilder = BatchPoints.database(this.config.database)
            .consistency(this.config.consistencyLevel);

    for (Map.Entry<PointKey, Map<String, Object>> values : builders.entrySet()) {
      final Point.Builder builder = Point.measurement(values.getKey().measurement);
      builder.time(values.getKey().time, this.config.precision);
      if (null != values.getKey().tags || values.getKey().tags.isEmpty()) {
        builder.tag(values.getKey().tags);
      }
      builder.fields(values.getValue());
      Point point = builder.build();
      if (log.isTraceEnabled()) {
        log.trace("put() - Adding point {}", point.toString());
      }
      batchBuilder.point(point);
    }

    BatchPoints batch = batchBuilder.build();
    this.influxDB.write(batch);
  }

  @Override
  public void stop() {
    if (null != this.influxDB) {
      log.info("stop() - Closing InfluxDB client.");
      this.influxDB.close();
    }
  }
}
