package sn.customtypes;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import sn.utils.Names;
import storm.trident.tuple.TridentTuple;

/**
 * Created by sumanthn on 5/4/14.
 */
public class DataVolumeAggregator extends CMSCombineAggregator {

    public DataVolumeAggregator() {
    }

    public DataVolumeAggregator(int keyCount) {
        super(keyCount);
    }

    @Override
    public CountMinSketch init(TridentTuple tuple) {

        CountMinSketch sketch = zero();
        long bytes = tuple.getLongByField(Names.BYTES_FLD);
        sketch.add(tuple.getStringByField(Names.HOSTNAME_FLD), bytes);

        return sketch;
    }
}
