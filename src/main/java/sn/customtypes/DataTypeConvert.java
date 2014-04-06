package sn.customtypes;

import backtype.storm.tuple.Values;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * Created by sumanthn on 2/4/14.
 */
public class DataTypeConvert implements Function {

    private final Object toConvertTo;

    public DataTypeConvert(Object toConvertTo) {
        this.toConvertTo = toConvertTo;
    }

    @Override
    public void execute(TridentTuple objects, TridentCollector collector) {
        for (int i = 0; i < objects.size(); i++) {
            String val = objects.getString(i);
            if (toConvertTo instanceof Integer) {
                collector.emit(new Values(Integer.valueOf(val)));
            } else if (toConvertTo instanceof Long) {
                collector.emit(new Values(Long.valueOf(val)));
            }
        }
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {

    }

    @Override
    public void cleanup() {

    }
}
