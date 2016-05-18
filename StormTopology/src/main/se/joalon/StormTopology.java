package se.joalon;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import kafka.api.OffsetRequest;
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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

public class StormTopology implements Serializable {

    public static final String TOPOLOGY_NAME = "Storm_Xbrl_Tag_Counter";

    public static void main (String[] args) {

//        String zookeeperIp = "192.168.56.101";
//        String hdfsHost = "hdfs://192.168.56.101:8020";
//        String nimbusHost = "192.168.56.101";

        String[] zookeeperIp = {"ip-172-31-33-201.eu-west-1.compute.internal"};
        String hdfsHost = "hdfs://ip-172-31-33-201.eu-west-1.compute.internal:8020";
        String nimbusHost = "ip-172-31-33-200.eu-west-1.compute.internal";


        String zookeeperHost = "ip-172-31-33-201.eu-west-1.compute.internal:2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        // Kafka spout config
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "one2", "/one2", "storm");
        kafkaConfig.startOffsetTime = OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;

        // Saving to hdfs bolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        SyncPolicy syncPolicy = new CountSyncPolicy(2);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64f, FileSizeRotationPolicy.Units.MB);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo");
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(hdfsHost)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        // Storm topology config
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", kafkaSpout, 1);
        builder.setBolt("XbrlParserBolt", new XbrlParserBolt(), 6).shuffleGrouping("KafkaSpout");
        builder.setBolt("HdfsPersistenceBolt", hdfsBolt, 30).shuffleGrouping("XbrlParserBolt");
        builder.setBolt("EventCounter", new CounterBolt(), 1).shuffleGrouping("HdfsPersistenceBolt");
        builder.setBolt("TestFinishedMessenger",
                new KafkaBolt<String,String>().withTopicSelector(new DefaultTopicSelector("realtime2"))
                        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String, String>(null,"done")), 1)
                .shuffleGrouping("EventCounter");

        // Storm Config
        Config config = new Config();
        config.setNumWorkers(1);
//        config.setMaxTaskParallelism(4);
        config.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.FALSE);
//        config.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 64*1024);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zookeeperIp));
        config.registerSerialization(String.class);

        // KafkaBolt properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "ip-172-31-33-199.eu-west-1.compute.internal:6667");
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