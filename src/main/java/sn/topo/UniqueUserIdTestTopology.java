package sn.topo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;
import sn.customtypes.DataTypeConvert;
import sn.customtypes.HLLBasedCombiner;
import sn.customtypes.HLLToStrConverter;
import sn.spout.SamevalGenerator;
import sn.utils.Names;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

import java.io.IOException;

/**
 * Created by sumanthn on 2/4/14.
 */
public class UniqueUserIdTestTopology extends AbstractTopology {

    public StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();

        SamevalGenerator dataGen = new SamevalGenerator();

        StateFactory mapState = new MemoryMapState.Factory();



        TridentState counterState = topology.newStream("CounterGen", dataGen)
                .groupBy(new Fields(Names.TIME_STAMP_FLD))
                .persistentAggregate(mapState, new Fields(Names.USER_ID_FLD),
                        new HLLBasedCombiner(Names.USER_ID_FLD),
                        new Fields("ItemCounter"));



        topology.newDRPCStream( "CountItemStream", localDRPC )

                .each(new Fields("args"), new Split(), new Fields("FLD"))
                .each(new Fields("FLD"), new DataTypeConvert(new Integer(1)), new Fields(Names.USER_ID_FLD))
                .each(new Fields(Names.USER_ID_FLD), new Debug())
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

        DateTime dateTime = new DateTime();
        System.out.println("Submit topology complete, waiting for start");
        Utils.sleep(6 * 1000);
        dateTime.minusSeconds(1);
        int groupById = 10;
        for(int i=0;i < 20;i++){



            System.out.println("Querying with key " + groupById);
            String rtnVals =  localDRPC.execute("CountItemStream" ,String.valueOf(groupById));
            System.out.println("Returned str is " +  rtnVals);

            try {
                HyperLogLog hll =HyperLogLog.Builder.build(Base64.decodeBase64(rtnVals));
                System.out.println("unique items for string " + hll.cardinality());
            } catch (IOException e) {
                e.printStackTrace();
            }

            Utils.sleep(5*1000);

        }


        shutdown();



    }

    public static void main(String [] args){

        UniqueUserIdTestTopology topo = new UniqueUserIdTestTopology();
        topo.kickStart();


    }



}
