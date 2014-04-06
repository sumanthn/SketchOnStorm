package sn.spout;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import sn.utils.TimeMeasures;
import storm.trident.spout.IBatchSpout;

import java.util.Map;
import java.util.Random;

/**
 * Created by sumanthn on 6/4/14.
 */
public abstract class AbstractDataGenerator implements IBatchSpout {

    protected Fields fields;

    protected boolean testMode = false;

    Random randomGen = new Random();
    int[] seeds = new int[]{2, 5, 14, 42, 132, 429, 1430, 4862,
            16796, 58786, 208012, 742900, 2674440, 9694845, 35357670, 129644790, 477638700, 1767263190};

    int batchSize = 100;
    long emitFrequency = TimeMeasures.ONESECOND_MILLIS;

    protected AbstractDataGenerator(int batchSize, long emitFrequency) {
        this.batchSize = batchSize;
        this.emitFrequency = emitFrequency;
    }

    protected AbstractDataGenerator(Fields fields) {
        this.fields = fields;
    }

    protected AbstractDataGenerator(Fields fields, int batchSize, long emitFrequency) {
        this.fields = fields;
        this.batchSize = batchSize;
        this.emitFrequency = emitFrequency;
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
