package sn.query;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.topo.StormClusterStore;
import sn.topo.UniqueUserCounterTopologyBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Handles all queries to the unique user id count
 * Created by sumanthn on 3/4/14.
 */
public class UserIdQueryHandler {

    private static UserIdQueryHandler ourInstance = new UserIdQueryHandler();

    public static UserIdQueryHandler getInstance() {
        return ourInstance;
    }

    private ScheduledExecutorService queryTask = Executors.newSingleThreadScheduledExecutor();

    private UserIdQueryHandler() {
    }

    class UserCountQueryTask implements Runnable {

        @Override
        public void run() {
            //for the previous min
            int queryForMin = DateTime.now().getMinuteOfDay() - 1;
            System.out.println("Launch Query for min " + queryForMin);
            fetchSketchForMin(queryForMin);

        }
    }

    public synchronized void init() {

        //run every one min and collect the data
        queryTask.scheduleAtFixedRate(new UserCountQueryTask(), 65L, 60L, TimeUnit.SECONDS);
    }

    public int fetchUniqueUserCount(final int minOfDay){
        HyperLogLog hllSketch = fetchSketchForMin(minOfDay);
        if (hllSketch!=null)
            hllSketch.cardinality();

        return -1;

    }

    //TODO: support multiple args for the DRPC and result processing
    public HyperLogLog fetchSketchForMin(final int minOfDay){
        System.out.println("Launch DRPC query for " +  minOfDay);
        //launch DRPC request
        //here it is immediate
        //TODO: support Async here
        String rtnVals = StormClusterStore.getInstance().getLocalDRPC().
                execute(UniqueUserCounterTopologyBuilder.DRPC_STREAM_NAME,
                        String.valueOf(minOfDay));


        try {
            HyperLogLog hll = HyperLogLog.Builder.build(Base64.decodeBase64(rtnVals));
            System.out.println("unique items for string " + hll.cardinality());
            return hll;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
