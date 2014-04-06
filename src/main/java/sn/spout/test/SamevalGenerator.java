package sn.spout.test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import sn.utils.Names;
import sn.utils.TimeMeasures;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.Map;

/**
 * Generates a stream of same values for a tuple
 * to test the combine aggregator of Stream Sketches
 * Created by sumanthn on 2/4/14.
 */
public class SamevalGenerator implements IBatchSpout {

    int startIdx = 1000;
    static int groupById = 10;

    Fields fields = new Fields(Names.TIME_STAMP_FLD, Names.USER_ID_FLD);

    @Override
    public void open(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void emitBatch(long l, TridentCollector collector) {

        collector.emit(new Values(Integer.valueOf(10), Integer.valueOf(startIdx)));
        startIdx++;
        // collector.emit(new Values(Integer.valueOf(10), Integer.valueOf(startIdx)));
        Utils.sleep(TimeMeasures.ONESECOND_MILLIS);
    }

    @Override
    public void ack(long l) {

    }

    @Override
    public void close() {

    }

    @Override
    public Map getComponentConfiguration() {
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
}
