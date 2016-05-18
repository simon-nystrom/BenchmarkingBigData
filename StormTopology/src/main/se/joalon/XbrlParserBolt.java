package se.joalon;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class XbrlParserBolt extends BaseRichBolt {

    public OutputCollector _output;

    private XBRLParser docStore;

    int solidityCount;
    private int counter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _output = outputCollector;
        docStore = new XBRLParser();
        solidityCount = 0;
//        counter = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            docStore.parseAndStoreDocument(tuple.getString(0));

            solidityCount += docStore.countByTag("se-gen-base:Soliditet");

//            System.out.println("XbrlParserBolt counter: " + ++counter);

            _output.emit(tuple, new Values(String.valueOf(solidityCount)));
//            _output.emit(new Values(String.valueOf(solidityCount)));
            _output.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Solidity"));
    }
}
