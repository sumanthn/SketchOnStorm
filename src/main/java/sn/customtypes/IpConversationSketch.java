package sn.customtypes;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.log4j.Logger;
import storm.trident.tuple.TridentTuple;

/**
 * Created by sumanthn on 4/4/14.
 */

public class IpConversationSketch extends HLLCombineAggregator {

    private static final Logger logger = Logger.getLogger(IpConversationSketch.class);

    private static final HashFunction hashFunction = Hashing.murmur3_128(3128);

    @Override
    public HyperLogLog init(TridentTuple tuple) {
        HyperLogLog hll = zero();

        hll.offerHashed(hashFunction.newHasher().putLong(tuple.getLong(0)).
                putLong(tuple.getLong(1)).hash().asLong());

        //    logger.trace("Current cardinality " + hll.cardinality() + " after conversation add");

        return hll;
    }
}
