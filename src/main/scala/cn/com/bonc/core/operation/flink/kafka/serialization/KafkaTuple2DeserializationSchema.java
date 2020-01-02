package cn.com.bonc.core.operation.flink.kafka.serialization;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

/**
 * @author wzq
 * @date 2019-09-26
 **/
public class KafkaTuple2DeserializationSchema implements KeyedDeserializationSchema<Tuple2<String, String>> {
    
    
    @Override
    public Tuple2<String, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        return new Tuple2<>(new String(messageKey), new String(message));
    }
    
    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
    }
}
