package sn;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.google.common.collect.Ordering;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by sumanthn on 4/4/14.
 */
public class CMSTest {

    class Server {

        int id;
        long count;

        public Server(int id, long count) {
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
    }

    @Test
    public void testCMSInit() {
        double epsOfTotalCount = 0.0001;
        double confidence = 0.99;
        int seed = 7364181;
        CountMinSketch sketch = new CountMinSketch(epsOfTotalCount, confidence, seed);

        int maxItems = 5 * 10 * 1000;
        Random random = new Random();

        random.setSeed(12102);
        for (int i = 0; i < maxItems; i++) {
            sketch.add(i, random.nextInt(12122));
        }

        List<Server> allServers = new ArrayList<Server>();
        for (int i = 0; i < maxItems; i++) {
            //    System.out.println(sketch.estimateCount(i));

            allServers.add(new Server(i, sketch.estimateCount(i)));
        }

        Ordering<Server> ordering = new Ordering<Server>() {

            @Override
            public int compare(Server server, Server server2) {
                return Long.compare(server.count, server2.count);
            }
        };

        List<Server> topKServers = ordering.greatestOf(allServers, 5);
        for (Server server : topKServers) {
            System.out.println(server.id + " counts " + server.count);
        }
    }
}
