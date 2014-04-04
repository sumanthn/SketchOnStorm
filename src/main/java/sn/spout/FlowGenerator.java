package sn.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;
import sn.utils.IpAddressUtils;
import sn.utils.Names;
import sn.utils.TimeMeasures;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.Map;
import java.util.Random;

/**
 * Generates Netflow like tuples
 * SourceIP, DestIp, Bytes,Packets
 * Created by sumanthn on 4/4/14.
 */
public class FlowGenerator implements IBatchSpout {

    Random randomGen = new Random();
    int[] seeds = new int[]{2, 5, 14, 42, 132, 429, 1430, 4862,
            16796, 58786, 208012, 742900, 2674440, 9694845, 35357670, 129644790, 477638700, 1767263190};

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
        int seed = seeds[randomGen.nextInt(seeds.length)];
        //will this result in some pattern in unique conversation??
        //randomGen.setSeed(seed);
        randomGen = new Random();
    }

    @Override
    public void emitBatch(long l, TridentCollector collector) {

        DateTime now = DateTime.now();
        for (int i = 0; i < batchSize; i++) {

            collector.emit(new Values(
                    IpAddressUtils.generateIp(randomGen), //SRC_IP
                    IpAddressUtils.generateIp(randomGen), //DEST_IP
                    Math.abs(randomGen.nextInt(Integer.MAX_VALUE / 2) + randomGen.nextInt(Integer.MAX_VALUE / 2)),//BYTES
                    randomGen.nextInt(325671), //pkts
                    now.getMinuteOfDay(), //mod
                    now.getHourOfDay()      //Hod
            ));
        }

        Utils.sleep(emitFrequency);
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
