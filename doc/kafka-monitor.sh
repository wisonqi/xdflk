#!/usr/bin/env bash

#该脚本通过计算本机器上kafka进程的数量来判断kafka运行是否正常

function monitor() {
    ip=$1
    count=`ssh ${ip} /data/jdk/bin/jps -ml| grep "kafka.Kafka /data/kafka_2.11-1.0.2/config/server.properties" | wc -l`
    if [[ ${count} -ne 1 ]]; then
        echo -e "\t${ip}机器上的kafka进程数量为：${count}，不正常，开始重启"
        ssh ${ip} /data/kafka_2.11-1.0.2/bin/kafka-server-stop.sh
        sleep 10
        ssh ${ip} /data/kafka_2.11-1.0.2/bin/kafka-server-start.sh -daemon /data/kafka_2.11-1.0.2/config/server.properties
        echo -e "\t${ip}机器上的kafka服务已重启完毕"
    else
        echo -e "\t${ip}机器上的kafka服务运行正常"
    fi
}

echo -e "\n\n监控开始时间：`date "+%Y-%m-%d %H:%M:%S"`"
ips=('172.16.125.25' '172.16.125.27' '172.16.125.156' '172.16.125.157')
for var in ${ips[@]} ; do
   monitor ${var}
done

