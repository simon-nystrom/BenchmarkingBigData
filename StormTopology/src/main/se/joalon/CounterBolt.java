package se.joalon;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class CounterBolt extends BaseRichBolt {

    private OutputCollector _output;
    private int counter;
    private int maxCounter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._output = outputCollector;
        this.counter = 0;
        this.maxCounter = 10000;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            if(++counter == maxCounter) {
                counter = 0;
                _output.emit(tuple, new Values("done with test messages"));
            }
            System.out.println("Counter: " + counter);
            _output.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("done"));
    }
}
