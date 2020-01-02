package cn.com.bonc;

import cn.com.bonc.core.operation.flink.RuleJson2Map;
import cn.com.bonc.entry.result.faultStandby.FaultStandbyData;
import cn.com.bonc.entry.result.warning.WarningData;
import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule;
import cn.com.bonc.utils.JedisUtils;
import cn.com.bonc.utils.SerializeUtils;
import cn.com.bonc.utils.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import redis.clients.jedis.ShardedJedis;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for simple App.
 */
public class AppTestJava {
    
    
    @Test
    public void test1() {
        String s = "1/2/3";
        String[] ss = s.split("/");
        System.out.println(ss.length);
    }
    
    /**
     * 查看Redis中的数据
     */
    @Test
    public void test2() {
        ShardedJedis jedis = JedisUtils.getJedis();
        String templateDevicePoint = "模板编码1~设备编码1~测点编码1";
        WarningData warnData = (WarningData) SerializeUtils.deserialize(jedis.get(templateDevicePoint.getBytes()));
        System.out.println(warnData);
    }
    
    @Test
    public void test3() {
        String temp = "/1/1";
        System.out.println(temp.split("/").length);
    }
    
    @Test
    public void test4() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String className = "cn.com.bonc.arithmetic.Compute";
        String methodName = "computer1";
        Class<?> aClass = Class.forName(className);
        Method method = aClass.getDeclaredMethod(methodName, Double.class, Double.class, Integer.class);
        Object result = method.invoke(aClass, 1.0, 2.2, 3);
        System.out.println(result);
        
    }
    
    @Test
    public void test5() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //必须指定
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getSimpleName()); //必须指定
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getSimpleName()); //必须指定
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AuditPartitioner.class.getSimpleName());
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        //消息发送成功
                    } else {
                        //消息发送失败
                        if (exception instanceof RetriableException) {
                            //处理可重试瞬时异常
                        } else {
                            //处理不可重试异常
                        }
                    }
                }
            });
            try {
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
    
    @Test
    public void test6() {
        long timestamp = 1572427756000L;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String format = formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("+8")));
        System.out.println(format);
    }
    
    @Test
    public void testFaultStandbyHandle() {
        String realKay = "1/1/1";
        String realValue = "{\n" +
                "    \"equipment\": \"1\",\n" +
                "    \"id\": \"1\",\n" +
                "    \"fields\": {\n" +
                "        \"1\": {\n" +
                "            \"time\": \"1572508664000\",\n" +
                "            \"value\": 0\n" +
                "        },\n" +
                "        \"2\": {\n" +
                "            \"time\": \"1572508123400\",\n" +
                "            \"value\": 0\n" +
                "        }\n" +
                "    },\n" +
                "    \"gateway\": \"1\"\n" +
                "}";
        String faultStandbyJson = "[\n" +
                "    {\n" +
                "        \"PROPCODE\": \"1\",\n" +
                "        \"PROPVALUE\": \"1\",\n" +
                "        \"KAFKA_TYPE\": \"1\",\n" +
                "        \"S_FLAG\": \"1\",\n" +
                "        \"_PK_\": \"_PK_1\",\n" +
                "        \"GREATER_THAN_ZERO\": \"0\",\n" +
                "        \"S_MUSER\": \"1\",\n" +
                "        \"S_MTIME\": \"1\",\n" +
                "        \"PRODCODE\": \"1\",\n" +
                "        \"CONSTANT_TYPE\": \"CONSTANT\",\n" +
                "        \"S_USER\": \"1\",\n" +
                "        \"_ROWNUM_\": 0,\n" +
                "        \"PROPNAME\": \"一\",\n" +
                "        \"ID\": \"1\",\n" +
                "        \"USE_TYPE\": \"FAULT\",\n" +
                "        \"JUDGE_TYPE\": \"=\",\n" +
                "        \"S_ATIME\": \"1\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"PROPCODE\": \"1\",\n" +
                "        \"PROPVALUE\": \"2\",\n" +
                "        \"KAFKA_TYPE\": \"1\",\n" +
                "        \"S_FLAG\": \"1\",\n" +
                "        \"_PK_\": \"_PK_11\",\n" +
                "        \"GREATER_THAN_ZERO\": \"1\",\n" +
                "        \"S_MUSER\": \"1\",\n" +
                "        \"S_MTIME\": \"1\",\n" +
                "        \"PRODCODE\": \"1\",\n" +
                "        \"CONSTANT_TYPE\": \"VARIABLE\",\n" +
                "        \"S_USER\": \"1\",\n" +
                "        \"_ROWNUM_\": 4,\n" +
                "        \"PROPNAME\": \"一一\",\n" +
                "        \"ID\": \"1\",\n" +
                "        \"USE_TYPE\": \"STANDBY\",\n" +
                "        \"JUDGE_TYPE\": \">\",\n" +
                "        \"S_ATIME\": \"1\"\n" +
                "    },\n" +
                "    {\n" +
                "        \"PROPCODE\": \"2\",\n" +
                "        \"PROPVALUE\": \"2\",\n" +
                "        \"KAFKA_TYPE\": \"2\",\n" +
                "        \"S_FLAG\": \"2\",\n" +
                "        \"_PK_\": \"2\",\n" +
                "        \"GREATER_THAN_ZERO\": \"2\",\n" +
                "        \"S_MUSER\": \"2\",\n" +
                "        \"S_MTIME\": \"2\",\n" +
                "        \"PRODCODE\": \"2\",\n" +
                "        \"CONSTANT_TYPE\": \"VARIABLE\",\n" +
                "        \"S_USER\": \"2\",\n" +
                "        \"_ROWNUM_\": 4,\n" +
                "        \"PROPNAME\": \"二\",\n" +
                "        \"ID\": \"2\",\n" +
                "        \"USE_TYPE\": \"STANDBY\",\n" +
                "        \"JUDGE_TYPE\": \"!=\",\n" +
                "        \"S_ATIME\": \"2\"\n" +
                "    }\n" +
                "]";
        String[] keys = realKay.split("/");
        Map<String, Object> fields = JSON.parseObject(realValue).getJSONObject("fields");
        ShardedJedis jedis = JedisUtils.getJedis();
        Tuple2<Set<String>, Map<String, Map<String, List<FaultStandbyRule>>>> tuple2 = RuleJson2Map.faultStandbyJsonHandle(faultStandbyJson);
        List<FaultStandbyData> faultStandbyDataList = new ArrayList<>();
//        FaultStandbyHandle.core(keys, fields, jedis, tuple2, faultStandbyDataList);
//        for (FaultStandbyData data : faultStandbyDataList) {
//            System.out.println(data);
//        }
    }
    
    public void aaa(FaultStandbyData data1, FaultStandbyData data2) {
        data1 = new FaultStandbyData();
        data2.setEndTime(1243454356L);
    }
    
    @Test
    public void test7() {
        FaultStandbyData data1 = null;
        FaultStandbyData data2 = new FaultStandbyData();
        aaa(data1, data2);
        System.out.println(data1);
        System.out.println(data2);
    }
    
    @Test
    public void testIsNumber() {
        System.out.println(StringUtils.isNumber("-1"));
        System.out.println(StringUtils.isNumber("-1.1"));
        System.out.println(StringUtils.isNumber("1"));
        System.out.println(StringUtils.isNumber("1.0"));
        System.out.println(StringUtils.isNumber("0.1"));
        System.out.println(StringUtils.isNumber("1.1"));
        System.out.println(StringUtils.isNumber("111"));
    }
    
    @Test
    public void test8() {
        String s = "{\n" +
                "    \"time\": \"1574239772000\",\n" +
                "    \"value\": 0\n" +
                "}";
        JSONObject jsonObject = JSON.parseObject(s);
        String value = jsonObject.get("value").toString();
        System.out.println(value);
        System.out.println(StringUtils.isNumber(value));
        System.out.println(Double.parseDouble(StringUtils.truncate(value)));
        
    }
    
    @Test
    public void test9() {
        String s = "{\"value1\": 1,\"value2\": \"2\"}";
        JSONObject jsonObject = JSON.parseObject(s);
        double value1 = jsonObject.getFloat("value1");
        double value2 = jsonObject.getFloat("value2");
        System.out.println(value1);
        System.out.println(value2);
    }
    
    @Test
    public void testHashMapToJson() {
        Map<String, String> map = new HashMap<>();
        map.put("1", null);
        map.put("2", "二");
        System.out.println(JSON.toJSONString(map));
    }
    
    
}
