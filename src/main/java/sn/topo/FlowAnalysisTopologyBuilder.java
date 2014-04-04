package sn.topo;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import sn.customtypes.DataTypeConvert;
import sn.customtypes.HLLToStrConverter;
import sn.customtypes.IpConversationSketch;
import sn.spout.FlowGenerator;
import sn.utils.Names;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * Created by sumanthn on 4/4/14.
 */
public class FlowAnalysisTopologyBuilder {

    public static final String STREAM_NAME = "IpFlow";
    public static final String STREAM_NAME2 = "IpFlow2";
    public static final String UNIQUE_CONVERSATION_COUNT = "UniqueConversationCount";
    public static final String CONVERSATION_COUNT = "ConversationCount";

    private FlowAnalysisTopologyBuilder() {

    }

    /**
     * Builds the topology for Unique UserId Counter
     */
    public static final StormTopology buildTopology() {

        //first init topology
        TridentTopology topology = new TridentTopology();

        //bring in the spout
        FlowGenerator dataGen = new FlowGenerator();

        // FlowGenTest dataGen = new FlowGenTest();

        //attach the state factory
        //here it is simple HashMap backed in memory store
        StateFactory mapState = new MemoryMapState.Factory();
        StateFactory conversationMapState = new MemoryMapState.Factory();

        //define the counter state
        //use HLL as sketch to keep track of unique userids
        Stream ipFlowStream = topology
                .newStream(STREAM_NAME, dataGen)
                .parallelismHint(4);

        TridentState counterState =

                ipFlowStream
                        //group by minutely bucket
                        .groupBy(new Fields(Names.MIN_OF_DAY_FLD))
                                //store the HLL based sketch for every minute
                                //this should give the unique user id count
                        .persistentAggregate(mapState, new Fields(Names.SOURCE_IP_FLD, Names.DEST_IP_FLD),
                                new IpConversationSketch(),
                                new Fields("ConversationsCount"));

        TridentState globalCountPerMin =
                ipFlowStream
                        //topology.newStream(STREAM_NAME2, dataGen)
                        .parallelismHint(4)

                                //group by minutely bucket
                        .groupBy(new Fields(Names.MIN_OF_DAY_FLD))
                                //store the HLL based sketch for every minute
                                //this should give the unique user id count
                        .persistentAggregate(conversationMapState, new Fields(Names.MIN_OF_DAY_FLD),
                                new Count(),
                                new Fields("ConversationCountPerMin"));

        //now define DRPC stream on which the queries are executed

        //attach to local instance of DRPC
        topology.newDRPCStream(UNIQUE_CONVERSATION_COUNT, StormClusterStore.getInstance().getLocalDRPC())

                //takes in string args the minute of day (bucket used for unique counts)
                .each(new Fields("args"), new Split(), new Fields("FLD"))
                        //MIN_OF_DAY_FLD is the key for the map
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)), new Fields(Names.MIN_OF_DAY_FLD))
                        //now get the fields
                .stateQuery(counterState, new Fields(Names.MIN_OF_DAY_FLD),
                        new MapGet(), new Fields(Names.COUNTER_VALS_FLD))

                        //filter out the NULLs
                .each(new Fields(Names.COUNTER_VALS_FLD), new FilterNull())

                        //convert the HLL sketch to Base64 encoded String
                        //since drpc.execute results can only be strings
                        //TODO: if possible define another combiner to combine multiple results from DRPC
                .each(new Fields(Names.COUNTER_VALS_FLD), new HLLToStrConverter(Names.COUNTER_VALS_FLD),
                        new Fields(Names.CONVERSATION_COUNT_FLD))
                .project(new Fields(Names.CONVERSATION_COUNT_FLD));

        topology.newDRPCStream(CONVERSATION_COUNT, StormClusterStore.getInstance().getLocalDRPC())

                //takes in string args the minute of day (bucket used for unique counts)
                .each(new Fields("args"), new Split(), new Fields("FLD"))
                        //MIN_OF_DAY_FLD is the key for the map
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)), new Fields(Names.MIN_OF_DAY_FLD))

                        //now get the fields
                .stateQuery(globalCountPerMin, new Fields(Names.MIN_OF_DAY_FLD),
                        new MapGet(), new Fields(Names.COUNTER_VALS_FLD))

                        //filter out the NULLs
                .each(new Fields(Names.COUNTER_VALS_FLD), new FilterNull());

        return topology.build();
    }
}