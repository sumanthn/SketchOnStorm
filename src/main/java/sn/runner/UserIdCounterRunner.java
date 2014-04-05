package sn.runner;

import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.joda.time.DateTime;
import sn.query.UserIdQueryHandler;
import sn.topo.StormClusterStore;
import sn.topo.UniqueUserCounterTopologyBuilder;
import sn.utils.TimeMeasures;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sumanthn on 3/4/14.
 */
public class UserIdCounterRunner {

    //duration for how many minutes in past
    static long getUniqueUserCount(int startMin, int duration) {
        List<HyperLogLog> sketches = new ArrayList<HyperLogLog>();
        HyperLogLog finalCounter = new HyperLogLog(16);

        int curMinofDay = startMin;

        //get for 5 mins
        //get all HLL Sketches for last 5 mins
        while (duration != 0) {

            System.out.println("Find for duration " + curMinofDay);
            HyperLogLog sketch = UserIdQueryHandler.getInstance().fetchSketchForMin(curMinofDay);
            //can directly merge with final counter
            //just in case merge exception occurs , we need to retry only with few sets
            if (sketch != null)
                sketches.add(sketch);

            curMinofDay--;
            duration--;
        }

        for (HyperLogLog hll : sketches) {
            System.out.println("Merging hll of cardinality " + hll.cardinality());
            try {
                finalCounter = (HyperLogLog) finalCounter.merge(hll);
                System.out.println("Cardinality now is " + finalCounter.cardinality());
            } catch (CardinalityMergeException e) {
                e.printStackTrace();
            }
        }

        //finalCounter.merge()

        return finalCounter.cardinality();
    }

    public static void main(String[] args) {
        StormClusterStore clusterStore = StormClusterStore.getInstance();
        clusterStore.init();

        System.out.println("Cluster is initiated");
        final String topoName = "UniqueUserCountTopo";
        clusterStore.addTopology(topoName, UniqueUserCounterTopologyBuilder.buildTopology());

        //spin off the query manage
        UserIdQueryHandler.getInstance().init();

        Utils.sleep((4 * TimeMeasures.ONEMIN_MILLIS) + 10);
        //launch  query

        long count = getUniqueUserCount(DateTime.now().getMinuteOfDay() - 1, 3);
        System.out.println("unique is " + count);
    }
}
