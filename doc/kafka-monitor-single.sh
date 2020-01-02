#!/usr/bin/env bash

#该脚本通过计算本机器上kafka进程的数量来判断kafka运行是否正常
echo -e "\n\n监控开始时间：`date "+%Y-%m-%d %H:%M:%S"`"
count=`/data/jdk/bin/jps -ml| grep "kafka.Kafka /data/kafka_2.11-1.0.2/config/server.properties" | wc -l`
if [[ ${count} -ne 1 ]]; then
    echo -e "kafka进程数量为：${count}，不正常，开始重启"
    /data/kafka_2.11-1.0.2/bin/kafka-server-start.sh -daemon /data/kafka_2.11-1.0.2/config/server.properties
    echo -e "kafka服务已重启完毕"
else
    echo -e "kafka服务运行正常"
fi

exit 0