package sn;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by sumanthn on 2/4/14.
 */
public class HLLTest {

    @Test
    public void hllSerializationTest() {
        HyperLogLog hll = new HyperLogLog(16);
        for (int i = 0; i < 10; i++) {
            hll.offer(i);
        }

        try {

            //this would check if we can return the data as string
            //and still be usable after we build HLL
            //both should use the same bytes and same hash function to merge cardinality

            String hllAsStr = Base64.encodeBase64String(hll.getBytes());

            HyperLogLog hll2 = HyperLogLog.Builder.build(Base64.decodeBase64(hllAsStr));
            assertEquals(hll.cardinality(), hll2.cardinality());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMerge() {

        HyperLogLog hll = new HyperLogLog(16);

        for (int i = 0; i < 10; i++) {
            hll.offer(i);
        }

        HyperLogLog hll2 = new HyperLogLog(16);
        for (int i = 1000; i < (1000 + 10); i++) {
            hll2.offer(i);
        }

        HyperLogLog hll3 = new HyperLogLog(16);

        assertEquals(hll.cardinality(), 10);
        assertEquals(hll2.cardinality(), 10);
        assertEquals(hll3.cardinality(), 0);

        try {
            ICardinality hllMerged = hll.merge(hll2);
            hllMerged = hllMerged.merge(hll3);
//            System.out.println(hllMerged.cardinality());

            assertEquals(hllMerged.cardinality(), 20);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testComplexHashBasedHLL() {
        HashFunction hashFunction = Hashing.murmur3_128(3128);

        int maxIpAddress = 1000 * 1000;
        Random randomGen = new Random();
        randomGen.setSeed(21718);
        long[] ipAddressArr1 = new long[maxIpAddress];
        long[] ipAddressArr2 = new long[maxIpAddress];

        long val3 = (long) Math.pow(256, 3);
        long val2 = (long) Math.pow(256, 2);
        long val1 = (long) Math.pow(256, 1);

        long[] multiplyer = new long[]{
                val3, val2, val1, 1
        };

        HashSet<Long> curSet = new HashSet<Long>();
        for (int i = 0; i < maxIpAddress; ) {

            long ipAddr1 = 0;
            long ipAddr2 = 0;
            for (int p = 0; p < 4; p++) {
                ipAddr1 = ipAddr1 + multiplyer[p] * randomGen.nextInt(255);
                ipAddr2 = ipAddr2 + multiplyer[p] * randomGen.nextInt(255);
            }

            if (!(curSet.contains(ipAddr1) ||
                    curSet.contains(ipAddr2))) {
                ipAddressArr1[i] = ipAddr1;
                ipAddressArr2[i] = ipAddr2;
                i++;
                curSet.add(ipAddr1);
                curSet.add(ipAddr2);
            }
        }

        HyperLogLog hll1 = new HyperLogLog(16);
        HyperLogLog hll2 = new HyperLogLog(16);

        for (int i = 0; i < maxIpAddress; i++) {
            long hashedVal = hashFunction.newHasher().putLong(ipAddressArr1[i]).
                    putLong(ipAddressArr2[i]).hash().asLong();
            hll1.offerHashed(hashedVal);
        }

        int forwardIdx = 0;
        int backwardIdx = maxIpAddress - 1;

        while (backwardIdx != 0) {
            long hashedVal = hashFunction.newHasher().putLong(ipAddressArr1[forwardIdx++]).
                    putLong(ipAddressArr2[backwardIdx--]).hash().asLong();
            hll2.offerHashed(hashedVal);
        }

        double count1 = Math.abs(hll1.cardinality() - maxIpAddress);
        double count2 = Math.abs(hll2.cardinality() - maxIpAddress);

        double pctError1 = count1 / maxIpAddress;
        double pctError2 = count2 / maxIpAddress;

        assertEquals("% error", 0.0, pctError1, 0.01);
        assertEquals("% error", 0.0, pctError2, 0.01);

        try {
            ICardinality hllMerged = hll1.merge(hll2);

            long actual = 2 * maxIpAddress;
            double pctError3 = ((double) Math.abs(hllMerged.cardinality() - actual)) / actual;
            assertEquals(0.01, pctError3, 0.02);
        } catch (CardinalityMergeException e) {
            e.printStackTrace();
        }
    }
}
