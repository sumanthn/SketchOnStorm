package sn.runner;

import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.collect.Ordering;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.topo.DataVolumeAnalysisTopologyBuilder;
import sn.topo.StormClusterStore;
import sn.utils.Globals;
import sn.utils.HostNameRepo;
import sn.utils.TimeMeasures;

import java.util.ArrayList;
import java.util.List;

/**
 * Runner for data volume analyzer
 * Queries for sketch every one min and lists out top10
 * Created by sumanthn on 6/4/14.
 */
public class DataVolumeTracker {

    public static void main(String[] args) {
        StormClusterStore clusterStore = StormClusterStore.getInstance();
        clusterStore.init();

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
                dumpTopK(cms, 10);
            }

            Utils.sleep(TimeMeasures.ONEMIN_MILLIS);
        }
    }

    /**
     * computes the TopK Data Volumes for the hosts
     */
    static void dumpTopK(final CountMinSketch cms, int topK) {
        List<HostDataVolume> hostDataVolumes = new ArrayList<HostDataVolume>();
        for (String hostName : HostNameRepo.getInstance().getHostNameKeys())
            if (cms.estimateCount(hostName) > 0) {
                // System.out.println("Volume by host:" + cms.estimateCount(hostName));
                hostDataVolumes.add(new HostDataVolume(hostName, cms.estimateCount(hostName)));
            }

        Ordering<HostDataVolume> dataVolumeRanker = new Ordering<HostDataVolume>() {

            @Override
            public int compare(HostDataVolume volume, HostDataVolume volumeOther) {
                return Long.compare(volume.getVolume(), volumeOther.getVolume());
            }
        };

        List<HostDataVolume> topKHostByVolume = dataVolumeRanker.greatestOf(hostDataVolumes, 10);
        int rank = 1;
        for (HostDataVolume host : topKHostByVolume) {
            System.out.println("Top " + rank + " " + host.toString());
            rank++;
        }
    }

    /**
     * utility class to get top 10
     */
    static class HostDataVolume {

        String hostName;

        long volume;

        HostDataVolume(String hostName, long volume) {
            this.hostName = hostName;
            this.volume = volume;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String hostName) {
            this.hostName = hostName;
        }

        public long getVolume() {
            return volume;
        }

        public void setVolume(int volume) {
            this.volume = volume;
        }

        public String toString() {
            return hostName + " Volume(bytes) " + volume;
        }
    }
}
