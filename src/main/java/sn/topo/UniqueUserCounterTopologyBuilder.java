package sn.topo;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import sn.customtypes.DataTypeConvert;
import sn.customtypes.HLLBasedCombiner;
import sn.customtypes.HLLToStrConverter;
import sn.spout.UserIdStreamGenerator;
import sn.utils.Names;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * Created by sumanthn on 3/4/14.
 */
public class UniqueUserCounterTopologyBuilder {

    public static final String STREAM_NAME="USERID_GENERATOR";
    public static final String DRPC_STREAM_NAME="CountItemStream";
    private UniqueUserCounterTopologyBuilder() {

    }

    /** Builds the topology for Unique UserId Counter*/
   public  static final StormTopology buildTopology() {

        //first init topology
        TridentTopology topology = new TridentTopology();

        //bring in the spout
        UserIdStreamGenerator dataGen = new UserIdStreamGenerator();

        //attach the state factory
        //here it is simple HashMap backed in memory store
        StateFactory mapState = new MemoryMapState.Factory();

        //define the counter state
        //use HLL as sketch to keep track of unique userids
        TridentState counterState =
                topology.newStream(STREAM_NAME, dataGen)
                //group by minutely bucket
                .groupBy(new Fields(Names.TIME_STAMP_FLD))
                //store the HLL based sketch for every minute
                //this should give the unique user id count
                .persistentAggregate(mapState, new Fields(Names.USER_ID_FLD),
                        new HLLBasedCombiner(Names.USER_ID_FLD),
                        new Fields("ItemCounter"));



        //now define DRPC stream on which the queries are executed

        //attach to local instance of DRPC
        topology.newDRPCStream(DRPC_STREAM_NAME, StormClusterStore.getInstance().getLocalDRPC() )

                //takes in string args the minute of day (bucket used for unique counts)
                .each(new Fields("args"), new Split(), new Fields("FLD"))
                //convert Str to integer each fld
                //USER_ID_FLD is the key for the map
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)), new Fields(Names.USER_ID_FLD))

                //.each(new Fields(Names.USER_ID_FLD), new Debug())
                //now get the fields
                .stateQuery(counterState, new Fields(Names.USER_ID_FLD), new MapGet(), new Fields(Names.COUNTER_VALS_FLD))

                //filter out the NULLs
                .each(new Fields(Names.COUNTER_VALS_FLD), new FilterNull())

                 //convert the HLL sketch to Base64 encoded String
                //since drpc.execute results can only be strings
                //TODO: if possible define another combiner to combine multiple results from DRPC
                .each(new Fields(Names.COUNTER_VALS_FLD), new HLLToStrConverter(Names.COUNTER_VALS_FLD),
                        new Fields(Names.UNIQUE_USER_SKETCH))
                .project(new Fields(Names.UNIQUE_USER_SKETCH));


        return topology.build();
    }
}
