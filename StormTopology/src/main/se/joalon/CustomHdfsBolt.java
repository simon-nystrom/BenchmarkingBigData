package se.joalon;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import java.io.IOException;
import java.util.Map;

public class CustomHdfsBolt extends HdfsBolt {

    private OutputCollector _output;
    private int counter;

    @Override
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        _output = collector;
        counter = 0;
        super.doPrepare(conf, topologyContext, collector);
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            super.execute(tuple);
            _output.emit(tuple, new Values(tuple.toString()));
            System.out.println("CustomHdfsBolt counter: " + ++counter);
//            _output.emit(new Values(tuple.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        super.declareOutputFields(outputFieldsDeclarer);
        outputFieldsDeclarer.declare(new Fields("done"));
    }
}