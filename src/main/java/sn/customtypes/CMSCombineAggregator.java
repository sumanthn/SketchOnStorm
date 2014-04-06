package sn.customtypes;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;
import storm.trident.operation.CombinerAggregator;

/**
 * Created by sumanthn on 5/4/14.
 */
public abstract class CMSCombineAggregator implements CombinerAggregator<CountMinSketch> {

    private int keyCount = 1000;

    protected CMSCombineAggregator() {
    }

    protected CMSCombineAggregator(int keyCount) {
        this.keyCount = keyCount;
    }

    @Override
    public CountMinSketch combine(CountMinSketch one, CountMinSketch other) {

        try {
            return CountMinSketch.merge(one, other);
        } catch (FrequencyMergeException e) {
            e.printStackTrace();
        }

        return zero();
    }

    @Override
    public CountMinSketch zero() {

        //make CMS
        return getCMS(keyCount);
    }

    protected CountMinSketch getCMS(int keysCount) {

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
}
