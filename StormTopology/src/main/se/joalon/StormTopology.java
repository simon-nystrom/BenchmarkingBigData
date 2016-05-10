package se.joalon;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class StormTopology {

    public static class LolBolt extends BaseRichBolt {

        public OutputCollector _output;

        private XBRLParser docStore;

        int solidityCount;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _output = outputCollector;
            docStore = new XBRLParser();
            solidityCount = 0;
        }

        @Override
        public void execute(Tuple tuple) {
            System.out.println(tuple.getFields());
            docStore.parseAndStoreDocument(tuple.getString(0));

            solidityCount += docStore.countByTag("se-gen-base:Soliditet");
            System.out.println("se-gen-base:Soliditet: " + solidityCount);

            _output.emit(tuple, new Values("Solidity", solidityCount));
            _output.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("lol"));
        }
    }

    public static final int PARALLELISM_HINT = 4;
    public static final String TOPOLOGY_NAME = "Storm_Xbrl_Tag_Counter";
    //public static final String KAFKA_TOPIC_NAME = "xbrl_spout";

    public static void main (String[] args) {

        String zookeeperIp = "192.168.56.101";
        String hdfsHost = "hdfs://192.168.56.101:8020";
        String nimbusHost = "192.168.56.101";
        String zookeeperHost = zookeeperIp +":2181";
        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        // Kafka spout config
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "noLols", "/noLols", "storm");
        kafkaConfig.startOffsetTime = OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.bufferSizeBytes = 1;

        // Saving to hdfs bolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");
        // Synchronize data buffer with the filesystem every 1000 tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        // Rotate data files when they reach five MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.GB);
        // Use default, Storm-generated file names
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo");


        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(hdfsHost)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        //Storm topology config
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("eventsEmitter", kafkaSpout, PARALLELISM_HINT);
        builder.setBolt("eventsProcessor", new LolBolt(), PARALLELISM_HINT).shuffleGrouping("eventsEmitter");
        builder.setBolt("hdfsPersistence", hdfsBolt, PARALLELISM_HINT).shuffleGrouping("eventsProcessor");

        // Storm Config
        Config config = new Config();
        config.setMaxTaskParallelism(PARALLELISM_HINT);
//        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zookeeperIp));

        // Kafka Config
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.56.101:6667");
        props.put("requests.required.acks", "all");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
