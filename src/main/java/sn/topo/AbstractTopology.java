package sn.topo;

import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;

/**
 *
 * Created by sumanthn on 2/4/14.
 */
public abstract  class AbstractTopology {

    protected String topologyName;
    protected LocalCluster localCluster;
    protected LocalDRPC localDRPC;


    protected void shutdown(){
        if (localCluster!=null)
            localCluster.shutdown();

        if (localDRPC!=null)
            localDRPC.shutdown();
    }


}
