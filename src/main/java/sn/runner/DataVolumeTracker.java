package sn.runner;

import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.topo.DataVolumeAnalysisTopologyBuilder;
import sn.topo.StormClusterStore;
import sn.utils.Globals;
import sn.utils.HostNameRepo;

/**
 * Created by sumanthn on 6/4/14.
 */
public class DataVolumeTracker {

    public static void main(String[] args) {
        StormClusterStore clusterStore = StormClusterStore.getInstance();
        clusterStore.init();

        System.out.println("Cluster is initiated");
        final String topoName = "DataVolumeTracker";
        clusterStore.addTopology(topoName, DataVolumeAnalysisTopologyBuilder.buildTopology());

        int maxHosts = 500;
        HostNameRepo.getInstance().generateNames(maxHosts);
        //spin off the query manage
        Utils.sleep(15 * 1000);

        for (int i = 0; i < 20; i++) {
            int queryKey = DateTime.now().getHourOfDay();
            System.out.println("Querying with key " + queryKey);
            String rtnVals = StormClusterStore.getInstance().getLocalDRPC()
                    .execute(DataVolumeAnalysisTopologyBuilder.DATA_VOLUME_BY_HOSTS,
                            String.valueOf(queryKey));

            if (rtnVals.equals(Globals.EMPTY_RESULT)) {
                System.out.println("No result for query on key " + queryKey);
            } else {
                CountMinSketch cms = CountMinSketch.deserialize((Base64.decodeBase64(rtnVals)));
                for (String hostName : HostNameRepo.getInstance().getHostNameKeys())
                    if (cms.estimateCount(hostName) > 0)
                        System.out.println("Volume by host:" + cms.estimateCount(hostName));
            }

            Utils.sleep(5 * 1000);
        }
    }
}
