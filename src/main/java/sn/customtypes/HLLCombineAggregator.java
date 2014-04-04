package sn.customtypes;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.log4j.Logger;
import storm.trident.operation.CombinerAggregator;

/**
 * Created by sumanthn on 4/4/14.
 */
public abstract class HLLCombineAggregator implements CombinerAggregator<HyperLogLog> {

    private static final Logger logger = Logger.getLogger(HLLCombineAggregator.class);

    protected int registerBits = 16;

    protected HLLCombineAggregator() {
    }

    protected HLLCombineAggregator(int registerBits) {
        this.registerBits = registerBits;
    }

    @Override
    public HyperLogLog combine(HyperLogLog one, HyperLogLog other) {
        try {
            // logger.trace("Combine called  merging now " + one.cardinality() + " cardinality of other " + other.cardinality());
            return (HyperLogLog) one.merge(other);
        } catch (CardinalityMergeException e) {
            logger.fatal("Cardinality merge exception " + e.getMessage());
            e.printStackTrace();
        }

        return zero();
    }

    @Override
    public HyperLogLog zero() {
        return new HyperLogLog(registerBits);
    }
}
