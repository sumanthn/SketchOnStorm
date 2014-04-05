package sn.customtypes;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import storm.trident.tuple.TridentTuple;

/**
 * An Aggregator based on HyperLogLog
 * Works with single field
 * Created by sumanthn on 5/4/14.
 */
public class HLLAggregator extends HLLCombineAggregator {

    private final String fieldName;

    public HLLAggregator(String fieldName) {
        this.fieldName = fieldName;
    }

    public HLLAggregator(int registerBits, String fieldName) {
        super(registerBits);
        this.fieldName = fieldName;
    }

    @Override
    public HyperLogLog init(TridentTuple tuple) {
        HyperLogLog hll = zero();
        //directly offer the object, internally the instance is checked
        // and the hash is computed based on type
        hll.offer(tuple.getValueByField(fieldName));
        return hll;
    }
}
