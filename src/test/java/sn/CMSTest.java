package sn;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;
import com.google.common.collect.Ordering;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.procedure.TIntProcedure;
import org.junit.Test;
import sn.utils.IpAddressUtils;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by sumanthn on 4/4/14.
 */
public class CMSTest {

    @Test
    public void testHeavyHitters() {
        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;
        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);

        int maxItems = 1000;
        Random random = new Random();
        TLongLongMap actualItemCount = new TLongLongHashMap(maxItems);

        random.setSeed(12102);
        for (int i = 0; i < maxItems; i++) {
            int itemCount = random.nextInt(20000);
            sketch.add(i, itemCount);
            actualItemCount.put(i, itemCount);
        }

        List<Item> itemsBagFromSketch = new ArrayList<Item>();
        List<Item> itemsBagActual = new ArrayList<Item>();

        for (int i = 0; i < maxItems; i++) {
            itemsBagFromSketch.add(new Item(i, sketch.estimateCount(i)));
            itemsBagActual.add(new Item(i, actualItemCount.get(i)));
        }

        Ordering<Item> ordering = new Ordering<Item>() {

            @Override
            public int compare(Item me, Item other) {
                return Long.compare(me.count, other.count);
            }
        };

        List<Item> topKItemsEstimated = ordering.greatestOf(itemsBagFromSketch, 5);
        List<Item> topKItemsActual = ordering.greatestOf(itemsBagActual, 5);
/*

        System.out.println("Top 5 estimated items");
        for (Item  item : topKItemsEstimated) {
            System.out.println(item);
        }
*/
/*
        System.out.println("Top 5 actual");*/
        for (int i = 0; i < topKItemsActual.size(); i++) {
            assertEquals(true, topKItemsActual.get(i).checkForEquality(topKItemsEstimated.get(i)));
        }
    }

    @Test
    public void testIncrementalAddition() {
        int maxKeys = 10;
        CountMinSketch cmsCounter = getCMS();
        for (int i = 0; i < maxKeys; i++)
            cmsCounter.add(i, 10);

        //additions should work
        //skew up 3 and 8
        cmsCounter.add(3, 19102);
        cmsCounter.add(8, 1012);

        //sprinkle more on others also
        cmsCounter.add(1, 1201);
        cmsCounter.add(2, 81);
        cmsCounter.add(5, 90);

        assertEquals(1211, cmsCounter.estimateCount(1), 2);
        assertEquals(19112, cmsCounter.estimateCount(3), 2);
        assertEquals(10, cmsCounter.estimateCount(4), 0);
    }

    @Test
    public void testWithBaseline() {

        int[] keySizes = new int[]{10, 100, 1000, 5000, 10000};
        for (int i = 0; i < keySizes.length; i++) {
            int maxKeys = keySizes.length;
            long[] serverIps = IpAddressUtils.generateIpSet(maxKeys, 10101);

            Random rgen = new Random();
            rgen.setSeed(3456);
            TLongLongMap actualCount = new TLongLongHashMap(maxKeys);
            CountMinSketch sketch1 = getCMS(maxKeys);
            //track counts
            for (long serverIp : serverIps) {
                int bytesServed = rgen.nextInt(20000);
                sketch1.add(serverIp, bytesServed);
                actualCount.put(serverIp, bytesServed);
            }

            for (long serverIp : serverIps) {
                assertEquals(sketch1.estimateCount(serverIp), actualCount.get(serverIp));
            }
        }
    }

    @Test
    public void testCMSMerge() {
        /** 50 CMS counters , with each 200 keys */

        //test merge and topk after merge
        int maxCategory = 10;
        int maxItemsPerCategory = 50;

        final Map<Integer, TIntArrayList> categoryToItemMap = new HashMap<Integer, TIntArrayList>();

        //maintain a map of counts per category
        final Map<Integer, TIntLongHashMap> actualCountStore = new HashMap<Integer, TIntLongHashMap>();

        final Map<Integer, CountMinSketch> sketchPerCategory = new HashMap<Integer, CountMinSketch>();
        CountMinSketch globalSketch = null;

        int itemIdIdx = 1000;
        for (int i = 0; i < maxCategory; i++) {

            //to be able to merge construct with same width , depth & seed
            sketchPerCategory.put(i, getCMS(maxItemsPerCategory));
            //init item map
            TIntArrayList itemIds = new TIntArrayList(maxItemsPerCategory);
            for (int p = 0; p < maxItemsPerCategory; p++)
                itemIds.add(p + itemIdIdx);

            categoryToItemMap.put(i, itemIds);

            itemIdIdx = itemIdIdx + (2 * maxItemsPerCategory);
            actualCountStore.put(i, new TIntLongHashMap(maxItemsPerCategory));
        }

        final Random random = new Random();
        random.setSeed(7091);

        //pump up data now
        //this will simulate in one second updates to an item
        for (int iter = 0; iter < 60; iter++) {
            for (Integer categoryId : categoryToItemMap.keySet()) {
                final int catId = categoryId;
                TIntArrayList itemIds = categoryToItemMap.get(categoryId);
                itemIds.forEach(new TIntProcedure() {

                    @Override
                    public boolean execute(int itemId) {
                        int hits = random.nextInt(10);
                        sketchPerCategory.get(catId).add(itemId, hits);
                        actualCountStore.get(catId).adjustOrPutValue(itemId, hits, hits);

                        return true;
                    }
                });
            }
        }
        final List<Item> itemsBagActual = new ArrayList<Item>();

        //check with baseline
        for (Integer categoryId : categoryToItemMap.keySet()) {
            final int catId = categoryId;

            TIntArrayList itemIds = categoryToItemMap.get(categoryId);
            itemIds.forEach(new TIntProcedure() {

                @Override
                public boolean execute(int itemId) {
                    assertEquals(sketchPerCategory.get(catId).estimateCount(itemId),
                            actualCountStore.get(catId).get(itemId));
                    itemsBagActual.add(new Item(itemId, actualCountStore.get(catId).get(itemId)));

                    return true;
                }
            });
            //break;

        }

        //merge all and figure out top10

        //booooring for loops
        CountMinSketch[] sketchArr = new CountMinSketch[maxCategory];
        int sktechIdx = 0;
        for (Integer categoryId : categoryToItemMap.keySet()) {
            sketchArr[sktechIdx++] = sketchPerCategory.get(categoryId);
        }

        try {
            globalSketch = CountMinSketch.merge(sketchArr);
        } catch (FrequencyMergeException e) {
            e.printStackTrace();
        }

        final List<Item> itemsBagEstimate = new ArrayList<Item>();
        //make top10 now
        List<Item> allItems = new ArrayList<Item>();
        for (Integer categoryId : categoryToItemMap.keySet()) {

            final int catId = categoryId;

            TIntArrayList itemIds = categoryToItemMap.get(categoryId);
            final CountMinSketch finalGlobalSketch = globalSketch;
            itemIds.forEach(new TIntProcedure() {

                @Override
                public boolean execute(int itemId) {
                    itemsBagEstimate.add(new Item(itemId, finalGlobalSketch.estimateCount(itemId)));
                    return true;
                }
            });
        }

        //now check ordering
        //some code repittion here
        Ordering<Item> ordering = new Ordering<Item>() {

            @Override
            public int compare(Item me, Item other) {
                return Long.compare(me.count, other.count);
            }
        };

        List<Item> topKItemsEstimated = ordering.greatestOf(itemsBagEstimate, 10);
        List<Item> topKItemsActual = ordering.greatestOf(itemsBagActual, 10);


        /*System.out.println("Top 10 estimated items");
        for (Item  item : topKItemsEstimated) {
            System.out.println(item);
        }


        System.out.println("Top 10 actual");*/
        for (int i = 0; i < topKItemsActual.size(); i++) {
            assertEquals(true, topKItemsActual.get(i).checkForEquality(topKItemsEstimated.get(i)));
        }
    }

    private CountMinSketch getCMS() {

        //double epsOfTotalCount = 0.00001;
        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;
        CountMinSketch cmsCounter = new CountMinSketch(epsOfTotalCount, confidence, seed);
        return cmsCounter;
    }

    private CountMinSketch getCMS(int keysCount) {

        double epsOfTotalCount = 0.00001;

        //trying to see if 1/keyscount*10 could work out
        if (keysCount < 1000)
            epsOfTotalCount = 0.001;
        else if (keysCount >= 1000 && keysCount < 10000)
            epsOfTotalCount = 0.0001;

        double confidence = 0.95;
        int seed = 7364181;
        CountMinSketch cmsCounter = new CountMinSketch(epsOfTotalCount, confidence, seed);
        return cmsCounter;
    }

    class Item {

        int id;
        long count;

        public Item(int id, long count) {
            this.id = id;
            this.count = count;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "id=" + id +
                    ", count=" + count +
                    '}';
        }

        boolean checkForEquality(final Item other) {
            if (other.count != count) return false;
            if (other.id != id) return false;

            return true;
        }
    }
}
