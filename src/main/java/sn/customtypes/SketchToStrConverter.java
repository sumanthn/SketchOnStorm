package sn.customtypes;

import backtype.storm.tuple.Values;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.commons.codec.binary.Base64;
import sn.utils.SketchType;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created by sumanthn on 6/4/14.
 */
public class SketchToStrConverter implements Function {

    private final SketchType sketchType;
    private final String fieldName;

    public SketchToStrConverter(SketchType sketchType, String fieldName) {
        this.sketchType = sketchType;
        this.fieldName = fieldName;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        byte[] sketchBytes = null;
        switch (sketchType) {

            case HLL:
                HyperLogLog hyperLogLog = (HyperLogLog) tuple.getValueByField(fieldName);

                try {
                    sketchBytes = hyperLogLog.getBytes();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case CMS:

                CountMinSketch cms = (CountMinSketch) tuple.getValueByField(fieldName);
                sketchBytes = CountMinSketch.serialize(cms);
                break;
        }

        String sketchAsStr = Base64.encodeBase64String(sketchBytes);
        collector.emit(new Values(sketchAsStr));
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
