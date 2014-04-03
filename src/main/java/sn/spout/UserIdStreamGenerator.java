package sn.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import sn.utils.Names;
import sn.utils.TimeMeasures;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Generates itemids from Itemset for counting
 * Created by sumanthn on 2/4/14.
 */
public class UserIdStreamGenerator implements IBatchSpout {


    private static final Logger logger = Logger.getLogger(UserIdStreamGenerator.class);


    int userIdMaxCount = 10*1000;
    int [] userIds = new int[userIdMaxCount];
    Random randomGenerator;
    DateTime cur;
    int batchSize = 1000;
    long emitFrequency = TimeMeasures.ONESECOND_MILLIS;



    public UserIdStreamGenerator(){}

    public UserIdStreamGenerator(int batchSize) {
        this.batchSize = batchSize;
    }

    public UserIdStreamGenerator(int batchSize, long emitFrequency) {
        this.batchSize = batchSize;
        this.emitFrequency = emitFrequency;
    }

    Fields fields =  new Fields(Names.TIME_STAMP_FLD, Names.USER_ID_FLD);
    @Override
    public void open(Map map, TopologyContext topologyContext) {


        randomGenerator = new Random();
      randomGenerator.setSeed(191229120);
        cur = new DateTime();
        for(int i=0;i < userIdMaxCount;i++){
            //there could be some reptition
            userIds[i] = Math.abs(randomGenerator.nextInt());

        }


    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {



        int minOfDay = cur.getMinuteOfDay();
        Set<Integer> hashSet = new HashSet<Integer>();

        for(int i=0;i < batchSize;i++){

            //emit minute of day and other data
            int itemId = userIds[randomGenerator.nextInt(userIdMaxCount)];
            hashSet.add(itemId);
            collector.emit(new Values(DateTime.now().getMinuteOfDay(), itemId));
        }

        //logger.info("Emitted batch :" + batchId + " tuples emitted " + batchSize + " Min of Day " + minOfDay);
       /* logger.info("Batch cardinality " + hashSet.size() + " min " + minOfDay
                + " from thread " +  Thread.currentThread().getId());
*/

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
