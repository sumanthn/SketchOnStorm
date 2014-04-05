package sn;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Created by sumanthn on 4/4/14.
 */
public class StreamSummaryTest {

    @Test
    public void testStreamSummaryAccuracy() {

        StreamSummary<Integer> vs = new StreamSummary<Integer>(5);

        Random random = new Random();
        random.setSeed(3128);
        int maxItems = 5;
        for (int i = 0; i < maxItems; i++) {
            vs.offer(i, random.nextInt(2000));
        }

        int fetchTopK = 5;
        List<Counter<Integer>> topK = vs.topK(fetchTopK);
        for (Counter<Integer> c : topK) {

            System.out.println(c.toString());
        }
    }

    @Test
    public void testStringBuckets() {
        StreamSummary<String> vs = new StreamSummary<String>(3);
        String[] stream = {"X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "C", "A", "A"};
        for (String i : stream) {
            vs.offer(i, 10);
        }
        List<Counter<String>> topK = vs.topK(3);
        for (Counter<String> c : topK) {
            // assertTrue(Arrays.asList("A", "C", "X").contains(c.getItem()));
            System.out.println(c.toString());
        }
    }
}
