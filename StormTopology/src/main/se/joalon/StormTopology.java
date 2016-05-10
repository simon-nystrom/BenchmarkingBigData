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

    public static void main (String[] args) {

        String zkIp = "192.168.56.101";

        String nimbusHost = "192.168.56.101";

        String zookeeperHost = zkIp +":2181";

        ZkHosts zkHosts = new ZkHosts(zookeeperHost);

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "noLols", "/noLols", "storm");
        kafkaConfig.startOffsetTime = OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.bufferSizeBytes = 1;

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("eventsEmitter", kafkaSpout, PARALLELISM_HINT);

        builder.setBolt("eventsProcessor", new LolBolt(), PARALLELISM_HINT).shuffleGrouping("eventsEmitter");

        Config config = new Config();

        // Storm Config
        config.setMaxTaskParallelism(PARALLELISM_HINT);
//        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);
        config.put(Config.NIMBUS_HOST, nimbusHost);
        config.put(Config.NIMBUS_THRIFT_PORT, 6627);
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkIp));

        // Kafka Config
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.56.101:6667");
        props.put("requests.required.acks", "all");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        try {
            StormSubmitter.submitTopology("Lol-Hacks", config, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
