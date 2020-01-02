package cn.com.bonc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author wzq
 * @date 2019-10-25
 **/
public class DataCount {
    
    @Test
    public void test1() throws InterruptedException {
        String topic = "iot_v1_property_up";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.125.25:9092,172.16.125.27:9092,172.16.125.156:9092,172.16.125.157:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        for (PartitionInfo info : partitionInfos) {
            topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
        }
        Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(topicPartitions);
        long startOffsetSum = 0;
        long endOffsetSum = 0;
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
            startOffsetSum += entry.getValue();
        }
        Thread.sleep(1000 * 60 * 3);
        topicPartitionLongMap = consumer.endOffsets(topicPartitions);
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap.entrySet()) {
            endOffsetSum += entry.getValue();
        }
        long difference = endOffsetSum - startOffsetSum;
        System.out.println("3分钟增加" + difference + "条数据");
        System.out.println("每秒增加" + difference / (60 * 3) + "条数据");
    }
    
    @Test
    public void test2() throws InterruptedException {
        String topic1 = "full_handle_data";
        String topic2 = "full_handle_data_a";
        String topic3 = "full_handle_data_b";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.125.25:9092,172.16.125.27:9092,172.16.125.156:9092,172.16.125.157:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitions1 = new ArrayList<>();
        List<PartitionInfo> partitionInfos1 = consumer1.partitionsFor(topic1);
        for (PartitionInfo info : partitionInfos1) {
            topicPartitions1.add(new TopicPartition(info.topic(), info.partition()));
        }
        Map<TopicPartition, Long> topicPartitionLongMap1 = consumer1.endOffsets(topicPartitions1);
        long startOffsetSum1 = 0;
        long endOffsetSum1 = 0;
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap1.entrySet()) {
            startOffsetSum1 += entry.getValue();
        }
        
        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitions2 = new ArrayList<>();
        List<PartitionInfo> partitionInfos2 = consumer2.partitionsFor(topic2);
        for (PartitionInfo info : partitionInfos2) {
            topicPartitions2.add(new TopicPartition(info.topic(), info.partition()));
        }
        Map<TopicPartition, Long> topicPartitionLongMap2 = consumer2.endOffsets(topicPartitions2);
        long startOffsetSum2 = 0;
        long endOffsetSum2 = 0;
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap2.entrySet()) {
            startOffsetSum2 += entry.getValue();
        }
        
        KafkaConsumer<String, String> consumer3 = new KafkaConsumer<>(props);
        List<TopicPartition> topicPartitions3 = new ArrayList<>();
        List<PartitionInfo> partitionInfos3 = consumer3.partitionsFor(topic3);
        for (PartitionInfo info : partitionInfos3) {
            topicPartitions3.add(new TopicPartition(info.topic(), info.partition()));
        }
        Map<TopicPartition, Long> topicPartitionLongMap3 = consumer3.endOffsets(topicPartitions3);
        long startOffsetSum3 = 0;
        long endOffsetSum3 = 0;
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap3.entrySet()) {
            startOffsetSum3 += entry.getValue();
        }
        
        Thread.sleep(1000 * 60 * 3);
        topicPartitionLongMap1 = consumer1.endOffsets(topicPartitions1);
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap1.entrySet()) {
            endOffsetSum1 += entry.getValue();
        }
        long difference1 = endOffsetSum1 - startOffsetSum1;
        consumer1.close();
        
        topicPartitionLongMap2 = consumer2.endOffsets(topicPartitions2);
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap2.entrySet()) {
            endOffsetSum2 += entry.getValue();
        }
        long difference2 = endOffsetSum2 - startOffsetSum2;
        consumer2.close();
        
        topicPartitionLongMap3 = consumer3.endOffsets(topicPartitions3);
        for (Map.Entry<TopicPartition, Long> entry : topicPartitionLongMap3.entrySet()) {
            endOffsetSum3 += entry.getValue();
        }
        long difference3 = endOffsetSum3 - startOffsetSum3;
        consumer3.close();
        
        long difference = difference1 + difference2 + difference3;
        System.out.println("3分钟增加" + difference + "条数据");
        System.out.println("每秒增加" + difference / (60 * 3) + "条数据");
    }
    
    @Test
    public void testProducerRealData() throws InterruptedException {
        String topic = "iot_v1_property_up";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.22.156:9092,172.16.22.168:9092,172.16.22.187:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int index = 0;
        String value;
        while (true) {
            String key = "1/1/1";
            if (index % 2 == 0) {
                value = "{\"equipment\":\"20\",\"id\":\"2399\",\"fields\":{\"1\":{\"time\":\"1567078756367\",\"value\":3},\"2\":{\"time\":\"1567078756367\",\"value\":3},\"3\":{\"time\":\"1567078756367\",\"value\":3},\"4\":{\"time\":\"1567078756367\",\"value\":3},\"5\":{\"time\":\"1567078756367\",\"value\":3},\"6\":{\"time\":\"1567078756367\",\"value\":3},\"7\":{\"time\":\"1567078756367\",\"value\":3},\"8\":{\"time\":\"1567078756367\",\"value\":3},\"9\":{\"time\":\"1567078756367\",\"value\":3},\"10\":{\"time\":\"1567078756367\",\"value\":3},\"11\":{\"time\":\"1567078756367\",\"value\":3},\"12\":{\"time\":\"1567078756367\",\"value\":3},\"13\":{\"time\":\"1567078756367\",\"value\":3},\"14\":{\"time\":\"1567078756367\",\"value\":3},\"15\":{\"time\":\"1567078756367\",\"value\":3},\"16\":{\"time\":\"1567078756367\",\"value\":3},\"17\":{\"time\":\"1567078756367\",\"value\":3},\"18\":{\"time\":\"1567078756367\",\"value\":3},\"19\":{\"time\":\"1567078756367\",\"value\":3},\"20\":{\"time\":\"1567078756367\",\"value\":3},\"21\":{\"time\":\"1567078756367\",\"value\":3},\"22\":{\"time\":\"1567078756367\",\"value\":3},\"23\":{\"time\":\"1567078756367\",\"value\":3},\"24\":{\"time\":\"1567078756367\",\"value\":3},\"25\":{\"time\":\"1567078756367\",\"value\":3},\"26\":{\"time\":\"1567078756367\",\"value\":3},\"27\":{\"time\":\"1567078756367\",\"value\":3},\"28\":{\"time\":\"1567078756367\",\"value\":3},\"29\":{\"time\":\"1567078756367\",\"value\":3},\"30\":{\"time\":\"1567078756367\",\"value\":3},\"31\":{\"time\":\"1567078756367\",\"value\":3},\"32\":{\"time\":\"1567078756367\",\"value\":3},\"33\":{\"time\":\"1567078756367\",\"value\":3},\"34\":{\"time\":\"1567078756367\",\"value\":3},\"35\":{\"time\":\"1567078756367\",\"value\":3},\"36\":{\"time\":\"1567078756367\",\"value\":3},\"37\":{\"time\":\"1567078756367\",\"value\":3},\"38\":{\"time\":\"1567078756367\",\"value\":3},\"39\":{\"time\":\"1567078756367\",\"value\":3},\"40\":{\"time\":\"1567078756367\",\"value\":3},\"41\":{\"time\":\"1567078756367\",\"value\":3},\"42\":{\"time\":\"1567078756367\",\"value\":3},\"43\":{\"time\":\"1567078756367\",\"value\":3},\"44\":{\"time\":\"1567078756367\",\"value\":3},\"45\":{\"time\":\"1567078756367\",\"value\":3},\"46\":{\"time\":\"1567078756367\",\"value\":3},\"47\":{\"time\":\"1567078756367\",\"value\":3},\"48\":{\"time\":\"1567078756367\",\"value\":3},\"49\":{\"time\":\"1567078756367\",\"value\":3},\"50\":{\"time\":\"1567078756367\",\"value\":3},\"51\":{\"time\":\"1567078756367\",\"value\":3},\"52\":{\"time\":\"1567078756367\",\"value\":3},\"53\":{\"time\":\"1567078756367\",\"value\":3},\"54\":{\"time\":\"1567078756367\",\"value\":3},\"55\":{\"time\":\"1567078756367\",\"value\":3},\"56\":{\"time\":\"1567078756367\",\"value\":3},\"57\":{\"time\":\"1567078756367\",\"value\":3},\"58\":{\"time\":\"1567078756367\",\"value\":3},\"59\":{\"time\":\"1567078756367\",\"value\":3},\"60\":{\"time\":\"1567078756367\",\"value\":3}},\"gateway\":\"TEST_MQTT_SIMULATOR\"}";
            } else {
                value = "{\"equipment\":\"20\",\"id\":\"2399\",\"fields\":{\"1\":{\"time\":\"1567078756367\",\"value\":0},\"2\":{\"time\":\"1567078756367\",\"value\":0},\"3\":{\"time\":\"1567078756367\",\"value\":0},\"4\":{\"time\":\"1567078756367\",\"value\":0},\"5\":{\"time\":\"1567078756367\",\"value\":0},\"6\":{\"time\":\"1567078756367\",\"value\":0},\"7\":{\"time\":\"1567078756367\",\"value\":0},\"8\":{\"time\":\"1567078756367\",\"value\":0},\"9\":{\"time\":\"1567078756367\",\"value\":0},\"10\":{\"time\":\"1567078756367\",\"value\":0},\"11\":{\"time\":\"1567078756367\",\"value\":0},\"12\":{\"time\":\"1567078756367\",\"value\":0},\"13\":{\"time\":\"1567078756367\",\"value\":0},\"14\":{\"time\":\"1567078756367\",\"value\":0},\"15\":{\"time\":\"1567078756367\",\"value\":0},\"16\":{\"time\":\"1567078756367\",\"value\":0},\"17\":{\"time\":\"1567078756367\",\"value\":0},\"18\":{\"time\":\"1567078756367\",\"value\":0},\"19\":{\"time\":\"1567078756367\",\"value\":0},\"20\":{\"time\":\"1567078756367\",\"value\":0},\"21\":{\"time\":\"1567078756367\",\"value\":0},\"22\":{\"time\":\"1567078756367\",\"value\":0},\"23\":{\"time\":\"1567078756367\",\"value\":0},\"24\":{\"time\":\"1567078756367\",\"value\":0},\"25\":{\"time\":\"1567078756367\",\"value\":0},\"26\":{\"time\":\"1567078756367\",\"value\":0},\"27\":{\"time\":\"1567078756367\",\"value\":0},\"28\":{\"time\":\"1567078756367\",\"value\":0},\"29\":{\"time\":\"1567078756367\",\"value\":0},\"30\":{\"time\":\"1567078756367\",\"value\":0},\"31\":{\"time\":\"1567078756367\",\"value\":0},\"32\":{\"time\":\"1567078756367\",\"value\":0},\"33\":{\"time\":\"1567078756367\",\"value\":0},\"34\":{\"time\":\"1567078756367\",\"value\":0},\"35\":{\"time\":\"1567078756367\",\"value\":0},\"36\":{\"time\":\"1567078756367\",\"value\":0},\"37\":{\"time\":\"1567078756367\",\"value\":0},\"38\":{\"time\":\"1567078756367\",\"value\":0},\"39\":{\"time\":\"1567078756367\",\"value\":0},\"40\":{\"time\":\"1567078756367\",\"value\":0},\"41\":{\"time\":\"1567078756367\",\"value\":0},\"42\":{\"time\":\"1567078756367\",\"value\":0},\"43\":{\"time\":\"1567078756367\",\"value\":0},\"44\":{\"time\":\"1567078756367\",\"value\":0},\"45\":{\"time\":\"1567078756367\",\"value\":0},\"46\":{\"time\":\"1567078756367\",\"value\":0},\"47\":{\"time\":\"1567078756367\",\"value\":0},\"48\":{\"time\":\"1567078756367\",\"value\":0},\"49\":{\"time\":\"1567078756367\",\"value\":0},\"50\":{\"time\":\"1567078756367\",\"value\":0},\"51\":{\"time\":\"1567078756367\",\"value\":0},\"52\":{\"time\":\"1567078756367\",\"value\":0},\"53\":{\"time\":\"1567078756367\",\"value\":0},\"54\":{\"time\":\"1567078756367\",\"value\":0},\"55\":{\"time\":\"1567078756367\",\"value\":0},\"56\":{\"time\":\"1567078756367\",\"value\":0},\"57\":{\"time\":\"1567078756367\",\"value\":0},\"58\":{\"time\":\"1567078756367\",\"value\":0},\"59\":{\"time\":\"1567078756367\",\"value\":0},\"60\":{\"time\":\"1567078756367\",\"value\":0}},\"gateway\":\"TEST_MQTT_SIMULATOR\"}";
            }
            index++;
            producer.send(new ProducerRecord<String, String>(topic, key, value));
            Thread.sleep(1000);
        }
    }
    
    @Test
    public void testConsumer() {
        String topic = "real_data_wzq";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "dev.cloudiip.bonc.local:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo info : partitionInfos) {
            topicPartitions.add(new TopicPartition(info.topic(), info.partition()));
        }
        consumer.assign(topicPartitions);
        long endOffset = 0;
        Map<TopicPartition, Long> endTopicPartitionLongMap = consumer.endOffsets(topicPartitions);
        Map<TopicPartition, Long> beginTopicPartitionLongMap = consumer.beginningOffsets(topicPartitions);
        for (Map.Entry<TopicPartition, Long> entry : endTopicPartitionLongMap.entrySet()) {
            endOffset = entry.getValue();
        }
        for (Map.Entry<TopicPartition, Long> entry : beginTopicPartitionLongMap.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue());
        }
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            long offset = 0;
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("topic:" + record.topic() + ", partition:" + record.partition() + ", offset:" + record.offset() + ", key:" + record.key());
                offset = record.offset();
            }
            if (offset == endOffset - 1) {
                break;
            }
        }
    }
    
    
}
