package popeye.hadoop.bulkload;

public class BulkLoadConstants {
  public static final String HBASE_ZK_CONNECT = "popeye.hadoop.bulkload.hbase.conf.hbase.zk.connect";
  public static final String UNIQUE_ID_TABLE_NAME = "popeye.hadoop.bulkload.hbase.uniqueid.table.name";
  public static final String UNIQUE_ID_CACHE_SIZE = "popeye.hadoop.bulkload.hbase.uniqueid.cachesize";
  public static final String MAX_DELAYED_POINTS = "popeye.hadoop.bulkload.hbase.max.delayed.points";
  public static final String TSDB_FORMAT_CONFIG = "popeye.hadoop.bulkload.tsdbformat.config";
  public static final String KAFKA_INPUTS = "popeye.hadoop.bulkload.kafka.inputs";
  public static final String KAFKA_BROKERS = "popeye.hadoop.bulkload.kafka.brokers";
  public static final String KAFKA_CONSUMER_TIMEOUT = "popeye.hadoop.bulkload.kafka.consumer.timeout";
  public static final String KAFKA_CONSUMER_BUFFER_SIZE = "popeye.hadoop.bulkload.kafka.consumer.buffer.size";
  public static final String KAFKA_CONSUMER_FETCH_SIZE = "popeye.hadoop.bulkload.kafka.consumer.fetch.size";
  public static final String KAFKA_CLIENT_ID = "popeye.hadoop.bulkload.kafka.consumer.client.id";
}
