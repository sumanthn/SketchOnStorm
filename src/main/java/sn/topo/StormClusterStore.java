package sn.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the storm cluster
 * Created by sumanthn on 3/4/14.
 */
public class StormClusterStore {

    private LocalCluster cluster;
    private Map<String,StormTopology> topologyMap;
    private LocalDRPC localDRPC;

    private static StormClusterStore ourInstance = new StormClusterStore();

    public static StormClusterStore getInstance() {
        return ourInstance;
    }

    private StormClusterStore() {
    }

    public synchronized void init(){
        cluster = new LocalCluster();
        localDRPC = new LocalDRPC();
        topologyMap = new HashMap<String, StormTopology>();
    }

    public synchronized void addTopology(final String topologyName, StormTopology topology){


        //currently no additional configuration is used
        Config conf = new Config();
        //conf.setDebug(true);
        conf.setMaxSpoutPending( 20 );

        topologyMap.put(topologyName, topology);
        cluster.submitTopology(topologyName,conf, topology);

    }

    public LocalDRPC getLocalDRPC() {
        return localDRPC;
    }
}
