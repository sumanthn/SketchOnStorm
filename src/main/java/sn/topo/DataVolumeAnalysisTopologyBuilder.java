package sn.topo;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import sn.customtypes.DataTypeConvert;
import sn.customtypes.DataVolumeAggregator;
import sn.customtypes.SketchToStrConverter;
import sn.spout.HostTrafficGenerator;
import sn.utils.Names;
import sn.utils.SketchType;
import sn.utils.TimeMeasures;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * Builds the Stream, State for computing
 * the data transmitted by a host
 * Created by sumanthn on 5/4/14.
 */
public class DataVolumeAnalysisTopologyBuilder {

    public static final String STREAM_NAME = "HostTrafficFlow";

    public static final String DATA_VOLUME_BY_HOSTS = "DataVolumeByHost";

    private DataVolumeAnalysisTopologyBuilder() {
    }

    public static final StormTopology buildTopology() {

        //first init topology

        TridentTopology topology = new TridentTopology();

        Fields fields = new Fields(Names.HOSTNAME_FLD, Names.BYTES_FLD, Names.HOUR_0F_DAY_FLD, Names.DAY_0F_YEAR_FLD);
        //bring in the spout
        HostTrafficGenerator dataGen = new HostTrafficGenerator(fields, 10,
                TimeMeasures.ONESECOND_MILLIS);

        //dataGen.turnOnTestMode();
        // FlowGenTest dataGen = new FlowGenTest();

        //attach the state factory
        //here it is simple HashMap backed in memory store
        StateFactory dataVolumeMapStore = new MemoryMapState.Factory();

        //define the counter state
        //use HLL as sketch to keep track of unique userids
        Stream hostTrafficStream = topology
                .newStream(STREAM_NAME, dataGen)
                .parallelismHint(1);

        TridentState counterState =

                hostTrafficStream
                        //group by minutely bucket
                        .groupBy(new Fields(Names.HOUR_0F_DAY_FLD))
                                //store the HLL based sketch for every minute
                                //this should give the unique user id count
                        .persistentAggregate(dataVolumeMapStore, new Fields(Names.HOSTNAME_FLD, Names.BYTES_FLD),
                                new DataVolumeAggregator(),
                                new Fields("DataVolumes"));

        //now define DRPC stream on which the queries are executed

        //attach to local instance of DRPC
        topology.newDRPCStream(DATA_VOLUME_BY_HOSTS, StormClusterStore.getInstance().getLocalDRPC())

                //takes in string args the minute of day (bucket used for unique counts)
                .each(new Fields(Names.ARGS_FLD), new Split(), new Fields("FLD"))
                        //MIN_OF_DAY_FLD is the key for the map
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)),
                        new Fields(Names.HOUR_0F_DAY_FLD))
                        //now get the fields
                .stateQuery(counterState, new Fields(Names.HOUR_0F_DAY_FLD),
                        new MapGet(), new Fields(Names.DATA_VOLUME_FLD))

                        //filter out the NULLs
                .each(new Fields(Names.DATA_VOLUME_FLD), new FilterNull())

                        //convert the HLL sketch to Base64 encoded String
                        //since drpc.execute results can only be strings
                        //TODO: if possible define another combiner to combine multiple results from DRPC
                .each(new Fields(Names.DATA_VOLUME_FLD), new SketchToStrConverter(SketchType.CMS,
                        Names.DATA_VOLUME_FLD),
                        new Fields(Names.DATA_VOLUME_SKETCH_FLD))
                .project(new Fields(Names.DATA_VOLUME_SKETCH_FLD));

        return topology.build();
    }
}
