package sn.utils;

import java.util.HashSet;
import java.util.Random;

/**
 * Created by sumanthn on 4/4/14.
 */
public class IpAddressUtils {

    private IpAddressUtils() {
    }

    static final Random randomGlob = new Random();
    static long val3 = (long) Math.pow(256, 3);
    static long val2 = (long) Math.pow(256, 2);
    static long val1 = (long) Math.pow(256, 1);

    static final long[] multiplyer = new long[]{
            val3, val2, val1, 1
    };

    public static long[] generateIpSet(int size, int seed) {

        Random randomGen = new Random();
        randomGen.setSeed(seed);

        long[] ipAddrArr = new long[size];

        HashSet<Long> curSet = new HashSet<Long>();
        for (int i = 0; i < size; ) {

            long ipAddr = 0;
            for (int p = 0; p < 4; p++)
                ipAddr = ipAddr + multiplyer[p] * randomGen.nextInt(255);

            if (!curSet.contains(ipAddr)) {
                ipAddrArr[i++] = ipAddr;
                curSet.add(ipAddr);
            }
        }
        return ipAddrArr;
    }

    public static long[] generateIpSet(int size) {
        Random randomGen = new Random();
        return generateIpSet(size, randomGen.nextInt());
    }

    public static long generateIp() {
        long ipAddr = 0;
        for (int p = 0; p < 4; p++)
            ipAddr = ipAddr + multiplyer[p] * randomGlob.nextInt(255);

        return ipAddr;
    }

    public static long generateIp(final Random random) {
        long ipAddr = 0;
        for (int p = 0; p < 4; p++)
            ipAddr = ipAddr + multiplyer[p] * random.nextInt(255);

        return ipAddr;
    }
}
