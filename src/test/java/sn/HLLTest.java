package sn;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * Created by sumanthn on 2/4/14.
 */
public class HLLTest {



    @Test
    public void hllSerializationTest(){
        HyperLogLog hll = new HyperLogLog(16);
        for(int i=0;i < 10;i++){
            hll.offer(i);
        }


        try {

            //this would check if we can return the data as string
            //and still be usable after we build HLL
           //both should use the same bytes and same hash function to merge cardinality

            String hllAsStr = Base64.encodeBase64String(hll.getBytes());

            HyperLogLog hll2 = HyperLogLog.Builder.build(Base64.decodeBase64(hllAsStr));
            assertEquals (hll.cardinality(), hll2.cardinality());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMerge(){



        HyperLogLog hll = new HyperLogLog(16);

        for(int i=0;i < 10;i++){
            hll.offer(i);
        }

        HyperLogLog hll2 = new HyperLogLog(16);
        for(int i=1000;i < (1000+10);i++){
            hll2.offer(i);
        }

        HyperLogLog hll3 = new HyperLogLog(16);


        assertEquals(hll.cardinality(),10);
        assertEquals(hll2.cardinality(),10);
        assertEquals(hll3.cardinality(),0);

        try {
            ICardinality hllMerged = hll.merge(hll2);
            hllMerged =  hllMerged.merge(hll3);
            System.out.println(hllMerged.cardinality());
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
    }

}
