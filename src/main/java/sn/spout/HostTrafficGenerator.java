package sn.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;
import sn.utils.HostNameRepo;
import storm.trident.operation.TridentCollector;

import java.util.Map;

/**
 * Generates IpFlow keeping track of known set of hosts which generate traffic
 * Created by sumanthn on 2/4/14.
 */
public class HostTrafficGenerator extends AbstractDataGenerator {

    int nextSeed = 0;

    //some of the many fields
    public HostTrafficGenerator(int batchSize, long emitFrequency) {
        super(batchSize, emitFrequency);
    }

    public HostTrafficGenerator(Fields fields) {
        super(fields);
    }

    public HostTrafficGenerator(Fields fields, int batchSize, long emitFrequency) {
        super(fields, batchSize, emitFrequency);
    }

    public void turnOnTestMode() {
        testMode = true;
    }

    public void turnOffTestMode() {
        testMode = false;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext) {
        //int seed = seeds[randomGen.nextInt(seeds.length)];
        //will this result in some pattern in unique conversation??
        //randomGen.setSeed(seed);

        nextSeed++;
        nextSeed = nextSeed % seeds.length;
        randomGen.setSeed(seeds[nextSeed]);
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

        HostNameRepo hostNameRepo = HostNameRepo.getInstance();
        DateTime now = DateTime.now();
        for (int i = 0; i < batchSize; i++) {

            if (!testMode) {
                collector.emit(new Values(
                        //host ip which generate traffic
                        hostNameRepo.getHostNameKeys().get(randomGen.nextInt(hostNameRepo.getHostNameKeys().size())),
                        //bytes generated
                        Long.valueOf(randomGen.nextInt(2020) + randomGen.nextInt(6732)),//BYTES
                        now.getHourOfDay(), //Hod
                        now.getDayOfYear() //Doy
                ));
            } else {
                collector.emit(new Values(
                        //host ip which generate traffic
                        "10.10.15.20",
                        //bytes generated
                        100L,
                        now.getHourOfDay(), //Hod
                        now.getDayOfYear() //Doy
                ));
            }
        }//end batch

        Utils.sleep(emitFrequency);
    }
}
