package cn.com.bonc;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author wzq
 * @date 2019-10-08
 **/
public class AuditPartitioner implements Partitioner {
    
    private Random random;
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String sKey = (String) key;
        List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        int partitionCount = partitionInfos.size();
        //将key值为 audit 的消息分配给最后一个分区，剩下的全部随机分配到其他分区
        return sKey == null || sKey.isEmpty() || !sKey.contains("audit") ? random.nextInt(partitionCount - 1) : partitionCount - 1;
    }
    
    /**
     * 实现必要的资源清理工作
     */
    @Override
    public void close() {
    }
    
    /**
     * 使用给定的键值对信息配置该类，初始化工作
     */
    @Override
    public void configure(Map<String, ?> configs) {
        random = new Random();
    }
}
