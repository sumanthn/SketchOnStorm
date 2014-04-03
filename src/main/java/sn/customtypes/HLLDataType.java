package sn.customtypes;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

/**
 * Created by sumanthn on 2/4/14.
 */
public class HLLDataType {

    private HyperLogLog hll;

    public HLLDataType(HyperLogLog hll) {
        this.hll = hll;
    }

    public String toString(){
        return "";
    }
}
