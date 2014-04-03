package sn.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Strings;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.customtypes.DataTypeConvert;
import sn.customtypes.HLLBasedCombiner;
import sn.customtypes.HLLToStrConverter;
import sn.spout.UserIdStreamGenerator;
import sn.utils.Names;
import sn.utils.TimeMeasures;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by sumanthn on 2/4/14.
 */
public class UniqueUserIdCountTopology extends AbstractTopology {

    /** cadrinality data by timestamp*/
    class CardinalityData{
        int minOfDay;
        HyperLogLog hll;
    }

    //runs every one min to capture the HLL
    // all data is present in Map, a simple DRPC request would fetch all the data
    class QueryTask implements Runnable{

        @Override
        public void run() {
            //for the previous min
            int queryForMin = DateTime.now().getMinuteOfDay()-1;
            System.out.println("Launch Query for min " + queryForMin);
            String rtnVals =  localDRPC.execute("CountItemStream" ,String.valueOf(queryForMin));

            if (!Strings.isNullOrEmpty(rtnVals)){

                System.out.println("returned back " + rtnVals);
            try {
                HyperLogLog hll =HyperLogLog.Builder.build(Base64.decodeBase64(rtnVals));
                System.out.println("unique items for string " + hll.cardinality());
            } catch (IOException e) {
                e.printStackTrace();
            }
            }
        }
    }
    private ScheduledExecutorService queryTask = Executors.newSingleThreadScheduledExecutor();
    private List<CardinalityData> cardinalityDataList = new ArrayList<CardinalityData>();



    public StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();

        UserIdStreamGenerator dataGen = new UserIdStreamGenerator(10,10* TimeMeasures.ONESECOND_MILLIS);

        StateFactory mapState = new MemoryMapState.Factory();



        TridentState counterState = topology.newStream("CounterGen", dataGen)
                .parallelismHint(4)
                .groupBy(new Fields(Names.TIME_STAMP_FLD))

                .persistentAggregate(mapState, new Fields(Names.USER_ID_FLD),
                        new HLLBasedCombiner(Names.USER_ID_FLD),
                        new Fields("ItemCounter"));



        topology.newDRPCStream("CountItemStream", localDRPC)

                .each(new Fields("args"), new Split(), new Fields("FLD"))
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)), new Fields(Names.USER_ID_FLD))
                //.parallelismHint(4)
                .each(new Fields(Names.USER_ID_FLD), new Debug())
                //.parallelismHint(4)
                .stateQuery(counterState, new Fields(Names.USER_ID_FLD), new MapGet(), new Fields(Names.COUNTER_VALS_FLD))
                .each(new Fields(Names.COUNTER_VALS_FLD), new FilterNull())

                        //.each(new Fields("CounterVals"), new HLLToStrConverter("CounterVals"), new Fields("UniqueItems"));
                .each(new Fields(Names.COUNTER_VALS_FLD), new HLLToStrConverter(Names.COUNTER_VALS_FLD), new Fields("UniqueItems"))
                .project(new Fields("UniqueItems"));

        return topology.build();
    }


    public void kickStart(){
        localDRPC= new LocalDRPC();


        localCluster = new LocalCluster();

        Config conf = new Config();
        //conf.setDebug(true);
        conf.setMaxSpoutPending( 20 );


        localCluster.submitTopology("CounterTopo", conf, buildTopology());

        //Utils.sleep(30 * TimeMeasures.ONESECOND_MILLIS);
        queryTask.scheduleAtFixedRate(new QueryTask(), 65L, 60L, TimeUnit.SECONDS);
        System.out.println("Submit topology complete, waiting for start");

        Utils.sleep(Long.MAX_VALUE);



        shutdown();



    }

    public static void main(String [] args){

        UniqueUserIdCountTopology topo = new UniqueUserIdCountTopology();
        topo.kickStart();


    }

}
