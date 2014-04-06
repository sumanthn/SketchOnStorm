package sn.topo;

/**
 * Created by sumanthn on 6/4/14.
 */
public class TopologyBuilderFactory {

    private static TopologyBuilderFactory ourInstance = new TopologyBuilderFactory();

    public static TopologyBuilderFactory getInstance() {
        return ourInstance;
    }

    private TopologyBuilderFactory() {
    }
}
