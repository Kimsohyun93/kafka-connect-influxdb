package platform.bada.v2.kafka.connect.influxdb;

import com.google.common.base.Strings;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.influxdb.InfluxDB;

import java.util.Map;
import java.util.concurrent.TimeUnit;


class InfluxDBSinkConnectorConfig extends AbstractConfig {

  public static final String DATABASE_CONF = "influxdb.database";
  static final String DATABASE_DOC = "The influxdb database to write to.";
  public static final String CONSISTENCY_LEVEL_CONF = "influxdb.consistency.level";
  static final String CONSISTENCY_LEVEL_DOC = "The default consistency level for writing data to InfluxDB.";
  public static final String TIMEUNIT_CONF = "influxdb.timeunit";
  static final String TIMEUNIT_DOC = "The default timeunit for writing data to InfluxDB.";
  public static final String LOG_LEVEL_CONF = "influxdb.log.level";
  static final String LOG_LEVEL_DOC = "influxdb.log.level";

  public static final String URL_CONF = "influxdb.url";
  static final String URL_DOC = "The url of the InfluxDB instance to write to.";

  public static final String USERNAME_CONF = "influxdb.username";
  static final String USERNAME_DOC = "The username to connect to InfluxDB with.";

  public static final String PASSWORD_CONF = "influxdb.password";
  static final String PASSWORD_DOC = "The password to connect to InfluxDB with.";

  public static final String GZIP_ENABLE_CONF = "influxdb.gzip.enable";
  static final String GZIP_ENABLE_DOC = "Flag to determine if gzip should be enabled.";

  public static final String KAFKA_URL_CONF = "kafka.url";
  static final String KAFKA_URL_DOC = "The url of the kafka instance to write to.";
  public static final String KAFKA_PORT_CONF = "kafka.port";
  static final String KAFKA_PORT_DOC = "The port of the kafka instance to write to.";

  public static final String REDIS_URL_CONF = "redis.url";
  static final String REDIS_URL_DOC = "The url of the redis instance to read the data model.";
  public static final String REDIS_PORT_CONF = "redis.port";
  static final String REDIS_PORT_DOC = "The port of the redis instance to read the data model.";


  public final String database;
  public final InfluxDB.ConsistencyLevel consistencyLevel;
  public final TimeUnit precision;
  public final InfluxDB.LogLevel logLevel;
  public final String url;
  public final String username;
  public final String password;
  public final boolean authentication;
  public final boolean gzipEnable;
  public final String kafkaUrl;
  public final Integer kafkaPort;
  static final int KAFKA_PORT_DEFAULT = 9092;
  public final String redisUrl;
  public final Integer redisPort;
  static final int REDIS_PORT_DEFAULT = 6379;

  public InfluxDBSinkConnectorConfig(Map<String, String> settings) {
    super(config(), settings);

    this.database = getString(DATABASE_CONF);
    this.consistencyLevel =  Enum.valueOf(InfluxDB.ConsistencyLevel.class, getString(CONSISTENCY_LEVEL_CONF));
    this.precision = Enum.valueOf(TimeUnit.class, getString(TIMEUNIT_CONF));
    this.logLevel = Enum.valueOf(InfluxDB.LogLevel.class, getString(LOG_LEVEL_CONF));

    this.url = getString(URL_CONF);
    this.username = getString(USERNAME_CONF);
    this.authentication = !Strings.isNullOrEmpty(this.username);
    this.password = getPassword(PASSWORD_CONF).value();
    this.gzipEnable = getBoolean(GZIP_ENABLE_CONF);

    this.kafkaUrl = getString(KAFKA_URL_CONF);
    this.kafkaPort = getInt(KAFKA_PORT_CONF);

    this.redisUrl = getString(REDIS_URL_CONF);
    this.redisPort = getInt(REDIS_PORT_CONF);
  }


  public static ConfigDef config() {
    return new ConfigDef()
            .define(URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, URL_DOC)
            .define(DATABASE_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, DATABASE_DOC)
            .define(USERNAME_CONF, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, USERNAME_DOC)
            .define(PASSWORD_CONF, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH, PASSWORD_DOC)
            .define(CONSISTENCY_LEVEL_CONF, ConfigDef.Type.STRING, InfluxDB.ConsistencyLevel.ONE.toString(), ConfigDef.Importance.MEDIUM, CONSISTENCY_LEVEL_DOC)
            .define(TIMEUNIT_CONF, ConfigDef.Type.STRING, TimeUnit.MILLISECONDS.toString(), ConfigDef.Importance.MEDIUM, TIMEUNIT_DOC)
            .define(LOG_LEVEL_CONF, ConfigDef.Type.STRING, InfluxDB.LogLevel.NONE.toString(), ConfigDef.Importance.LOW, LOG_LEVEL_DOC)
            .define(GZIP_ENABLE_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW, GZIP_ENABLE_DOC)
            .define(KAFKA_URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, KAFKA_URL_DOC)
            .define(KAFKA_PORT_CONF, ConfigDef.Type.INT, KAFKA_PORT_DEFAULT, ConfigDef.Importance.MEDIUM, KAFKA_PORT_DOC)
            .define(REDIS_URL_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, REDIS_URL_DOC)
            .define(REDIS_PORT_CONF, ConfigDef.Type.INT, REDIS_PORT_DEFAULT, ConfigDef.Importance.MEDIUM, REDIS_PORT_DOC);
  }

}
