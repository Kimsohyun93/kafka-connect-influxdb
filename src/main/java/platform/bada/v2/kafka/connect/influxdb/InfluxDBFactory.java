package platform.bada.v2.kafka.connect.influxdb;


import org.influxdb.InfluxDB;

interface InfluxDBFactory {
  InfluxDB create(InfluxDBSinkConnectorConfig config);
}
