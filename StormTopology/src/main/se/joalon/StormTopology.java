package se.joalon;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;
import kafka.api.OffsetRequest;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.trident.spout.RichSpoutBatchExecutor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

public class StormTopology implements Serializable {

    public static final String TOPOLOGY_NAME = "Storm_Xbrl_Tag_Counter";
    //public static final String KAFKA_TOPIC_NAME = "xbrl_spout";

    public static void main (String[] args) {

//        String zookeeperIp = "192.168.56.101";
//        String hdfsHost = "hdfs://192.168.56.101:8020";
//        String nimbusHost = "192.168.56.101";

        String zookeeperIp = "52.51.158.75";
        String hdfsHost = "hdfs://52.51.158.75:8020";
        String nimbusHost = "52.31.53.77";


        String zookeeperHost = zookeeperIp +":2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        // Kafka spout config
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "test1", "/test1", "storm");
        kafkaConfig.startOffsetTime = OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.bufferSizeBytes = 4;

        // Saving to hdfs bolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        SyncPolicy syncPolicy = new CountSyncPolicy(2);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo");
        HdfsBolt hdfsBolt = new CustomHdfsBolt()
                .withFsUrl(hdfsHost)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        // Storm topology config
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("eventsEmitter", kafkaSpout, 1);
        builder.setBolt("eventsProcessor", new XbrlParserBolt(), 7).shuffleGrouping("eventsEmitter");
        builder.setBolt("hdfsPersistence", hdfsBolt, 30).shuffleGrouping("eventsProcessor");
        builder.setBolt("eventCounter", new CounterBolt(), 1).globalGrouping("hdfsPersistence");
        builder.setBolt("TestFinishedMessenger",
                new KafkaBolt<String,String>().withTopicSelector(new DefaultTopicSelector("time"))
                        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>(null,"done"))
        ).globalGrouping("eventCounter");

        // Storm Config
        Config config = new Config();
//        config.setMaxTaskParallelism(4);
        config.setNumWorkers(1);
//        config.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.FALSE);
//        config.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 64*1024);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
//        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zookeeperIp));
        config.registerSerialization(String.class);

        // KafkaBolt properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "52.51.188.130:6667");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}