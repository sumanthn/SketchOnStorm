package sn.runner;

import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.topo.FlowAnalysisTopologyBuilder;
import sn.topo.StormClusterStore;
import sn.utils.Globals;

import java.io.IOException;

/**
 * Created by sumanthn on 4/4/14.
 */
public class FlowAnalyzer {

    public static void main(String[] args) {
        StormClusterStore clusterStore = StormClusterStore.getInstance();
        clusterStore.init();

        System.out.println("Cluster is initiated");
        final String topoName = "FlowAnalysis";
        clusterStore.addTopology(topoName, FlowAnalysisTopologyBuilder.buildTopology());

        //spin off the query manage
        Utils.sleep(15 * 1000);

        for (int i = 0; i < 20; i++) {
            int queryKey = DateTime.now().getMinuteOfDay();
            System.out.println("Querying with key " + queryKey);
            String rtnVals = StormClusterStore.getInstance().getLocalDRPC()
                    .execute(FlowAnalysisTopologyBuilder.UNIQUE_CONVERSATION_COUNT,
                            String.valueOf(queryKey));

            if (rtnVals.equals(Globals.EMPTY_RESULT)) {
                System.out.println("No result for query on key " + queryKey);
            } else {
                try {
                    HyperLogLog hll = HyperLogLog.Builder.build(Base64.decodeBase64(rtnVals));
                    System.out.println("Unique Conversations count " + hll.cardinality());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            String conversationCount = StormClusterStore.getInstance().getLocalDRPC()
                    .execute(FlowAnalysisTopologyBuilder.CONVERSATION_COUNT,
                            String.valueOf(queryKey));

            if (!conversationCount.equals(Globals.EMPTY_RESULT)) {
                System.out.println("All Flow conversations " + conversationCount);
            }

            Utils.sleep(5 * 1000);
        }
    }
}
