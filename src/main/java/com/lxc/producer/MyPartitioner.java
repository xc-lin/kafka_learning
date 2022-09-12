package com.lxc.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author Frank_lin
 * @date 2022/9/12
 */
public class MyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取数据atguigu hello
        System.out.println(111);
        String msgString = value.toString();
        int partition = 1;
        if (msgString.contains("linxc")) {
            partition = 2;
        }
        return partition;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
