#!/usr/bin/env bash

#该脚本通过计算四台flink机器上有关flink进程的数量来判断flink集群运行是否正常

echo -e "\n\n监控开始时间：`date "+%Y-%m-%d %H:%M:%S"`"
count29=`ssh 172.16.125.29 /data/app/jdk/bin/jps -ml | grep flink | wc -l`
echo -e "\t29机器上的flink进程数量为：${count29}"
count31=`ssh 172.16.125.31 /data/app/jdk/bin/jps -ml | grep flink | wc -l`
echo -e "\t31机器上的flink进程数量为：${count31}"
count161=`ssh 172.16.125.161 /data/app/jdk/bin/jps -ml | grep flink | wc -l`
echo -e "\t161机器上的flink进程数量为：${count161}"
count162=`ssh 172.16.125.162 /data/app/jdk/bin/jps -ml | grep flink | wc -l`
echo -e "\t162机器上的flink进程数量为：${count162}"

let count=${count29}+${count31}+${count161}+${count162}

if [[ $count -ne 5 ]]; then
    echo -e "\t集群运行异常，现在停止集群运行，20秒之后重启集群，再过20秒向集群提交报警处理和模拟点计算任务。"
    /data/app/flink/bin/stop-cluster.sh
    sleep 20s
    /data/app/flink/bin/start-cluster.sh
    sleep 20s
    nohup /data/app/flink/bin/flink run -c cn.com.bonc.core.WarningMain /data/programFiles/xingda_warning_handle/xingda-warning-5.2-SNAPSHOT-jar-with-dependencies.jar &
    nohup /data/app/flink/bin/flink run -c com.cloudiip.lab.calculation.engine.FlinkBootstrap /data/calc-flink/cloudiip-lab-calculation-engine-1.0-SNAPSHOT-fat-test.jar calcTask /data/calc-flink/config.properties &
    echo -e "\t集群重启完毕，xingda-warning-5.2-SNAPSHOT-jar-with-dependencies.jar和cloudiip-lab-calculation-engine-1.0-SNAPSHOT-fat-test.jar任务均已提交"
else
    echo -e "\t集群运行正常"
fi

exit 0