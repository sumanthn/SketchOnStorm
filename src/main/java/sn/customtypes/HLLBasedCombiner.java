package sn.customtypes;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.log4j.Logger;
import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Estimate cardinality based on HLL
 * <p/>
 * Created by sumanthn on 2/4/14.
 */
//TODO: Use Linear counting or Adaptive counting for lower cadrinality sets
    //TODO: HLLPlus would also work out for lower cadrinality sets
public class HLLBasedCombiner implements CombinerAggregator<HyperLogLog> {

    private static final Logger logger = Logger.getLogger(HLLBasedCombiner.class);

    private String fieldName;

    public HLLBasedCombiner(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public HyperLogLog init(TridentTuple tuple) {
        HyperLogLog hll = zero();
        final Integer itemId = tuple.getIntegerByField(fieldName);
        // System.out.println("working on item id " +itemId);

        hll.offer(itemId);
        logger.trace("Current cardinality " + hll.cardinality() + " after item add " + itemId);
        return hll;
    }

    @Override
    public HyperLogLog combine(HyperLogLog one, HyperLogLog other) {

        try {
            logger.trace("Combine called  merging now " + one.cardinality() + " cardinality of other " + other.cardinality());
            return (HyperLogLog) one.merge(other);
        } catch (CardinalityMergeException e) {
            logger.fatal("Cardinality merge exception " + e.getMessage());
            e.printStackTrace();
        }

        return zero();
    }

    @Override
    public HyperLogLog zero() {

        return new HyperLogLog(16);
    }
}
