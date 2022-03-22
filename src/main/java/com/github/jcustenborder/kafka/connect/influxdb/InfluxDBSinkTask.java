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
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.common.base.Strings;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

//import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
     * Mobius CIN Return Data
     *
     * {
     *   "op": 5,
     *   "rqi": "auzOyAA5eTj",
     *   "to": "kafka://localhost:9092/timeseries",
     *   "fr": "/Mobius2",
     *   "pc": {
     *     "m2m:sgn": {
     *       "sur": "Mobius/kafka_ae/kafka_cnt/storageOptions",
     *       "nev": {
     *         "rep": {
     *           "m2m:cin": {
     *             "rn": "4-202203100748514337203",
     *             "ty": 4,
     *             "pi": "3-20220310061854025127",
     *             "ri": "4-20220310074851434990",
     *             "ct": "20220310T074851",
     *             "lt": "20220310T074851",
     *             "st": 29,
     *             "et": "20240310T074851",
     *             "cs": 113,
     *             "con": {
     *               "TAG_ID": 8065243138,
     *               "POS_TYPE": 1,
     *               "POS_TIME": "2021-10-14 08:53:30.096",
     *               "XPOS": 42.7068856917,
     *               "YPOS": 2.3179656097
     *             },
     *             "cr": "S20170717074825768bp2l"
     *           }
     *         },
     *         "net": 3
     *       },
     *       "rvi": "2a"
     *     }
     *   }
     * }
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
      JSONObject cinData = new JSONObject();
      try {
        cinData = (JSONObject) jParser.parse(record.value().toString());
      } catch (ParseException e) {
        e.printStackTrace();
      }
      System.out.println("THIS IS VALUE OF JSONObject : " + cinData);

      String cinSUR = (String) ((JSONObject) ((JSONObject) cinData.get("pc")).get("m2m:sgn")).get("sur");
      String[] surArr = cinSUR.split("/");
      String measurement = surArr[1];
      System.out.println("THIS IS VALUE OF MEASUREMENT : " + measurement);
      if (Strings.isNullOrEmpty(measurement.toString())) {
        throw new DataException("measurement is a required field.");
      }

      final Map<String, String> tags = new HashMap<String, String>();
      tags.put("container", surArr[2]);
      System.out.println("THIS IS VALUE OF CONTAINER : " + tags.toString());

      final long time = record.timestamp();
      PointKey key = PointKey.of(measurement, time, tags);
      Map<String, Object> fields = builders.computeIfAbsent(key, pointKey -> new HashMap<>(100));

      JSONObject dataField = (JSONObject) ((JSONObject) ((JSONObject) ((JSONObject) ((JSONObject) cinData.get("pc")).get("m2m:sgn")).get("nev")).get("rep")).get("m2m:cin");
      System.out.println("THIS IS VALUE OF Data Fields : " + dataField);
      try {
        /**
         * flatten nested data field & Get Parsed Creation Time
         */

        String creationTime = (String) dataField.get("ct");
        SimpleDateFormat  dateParser  = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
        SimpleDateFormat  dateFormatter   = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date parsedTime = dateParser.parse(creationTime);
        creationTime = dateFormatter.format(parsedTime);

        JSONObject flattenedDataField = (JSONObject) jParser.parse(JsonFlattener.flatten(dataField.get("con").toString()));
        flattenedDataField.put("creation_time", creationTime);
        System.out.println("THIS IS VALUE OF FLATTENED JSON : " + flattenedDataField);

        ArrayList<String> fieldKeys = new ArrayList<String>(flattenedDataField.keySet());
        System.out.println("THIS IS VALUE OF Data Fields KEY SET : " + flattenedDataField.keySet());

        for (String fieldKey : fieldKeys) {
          System.out.println("THIS IS VALUE OF Data Fields KEYs : " + fieldKey);
          fields.put(fieldKey, flattenedDataField.get(fieldKey));
        }
      } catch (ParseException e) {
        e.printStackTrace();
      } catch (java.text.ParseException e) {
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
