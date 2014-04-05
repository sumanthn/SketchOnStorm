package sn.customtypes;

import backtype.storm.tuple.Values;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.commons.codec.binary.Base64;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.Map;

/**
 * Converts HLL to bytes to Binary64 Encoded String
 * DRPC returns back only string
 * Created by sumanthn on 2/4/14.
 */
public class HLLToStrConverter implements Function {

    private final String fieldName;

    public HLLToStrConverter(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        HyperLogLog hyperLogLog = (HyperLogLog) tuple.getValueByField(fieldName);

        try {
            byte[] hllBytes = hyperLogLog.getBytes();
            String hllStr = Base64.encodeBase64String(hllBytes);
            collector.emit(new Values(hllStr));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
