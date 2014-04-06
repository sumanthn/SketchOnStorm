package sn.spout.test;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;
import sn.utils.Names;
import sn.utils.TimeMeasures;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.Map;
import java.util.Random;

/**
 * Created by sumanthn on 4/4/14.
 */
public class FlowGenTest implements IBatchSpout {

    Random randomGen = new Random();

    int batchSize = 100;
    long emitFrequency = TimeMeasures.ONESECOND_MILLIS;

    Fields fields = new Fields(Names.SOURCE_IP_FLD,
            Names.DEST_IP_FLD,
            Names.BYTES_FLD,
            Names.PKT_COUNT_FLD,
            Names.MIN_OF_DAY_FLD,
            Names.HOUR_0F_DAY_FLD
    );

    @Override
    public void open(Map map, TopologyContext topologyContext) {
        randomGen.setSeed(8192);
    }

    @Override
    public void emitBatch(long l, TridentCollector collector) {

        DateTime now = DateTime.now();

        collector.emit(new Values(
                2130706433L, //SRC_IP
                4294967040L, //DEST_IP
                8192,
                39,
                1002, //mod
                now.getHourOfDay()      //Hod

        ));
        collector.emit(new Values(
                4294967040L,//SRC_IP
                2130706433L,
                8192,
                39,
                1002, //mod
                now.getHourOfDay()      //Hod

        ));
        Utils.sleep(emitFrequency);
    }

    @Override
    public void ack(long batchId) {

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
