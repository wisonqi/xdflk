package cn.com.bonc.core.operation.handle;

import cn.com.bonc.entry.result.faultStandby.FaultStandbyData;
import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule;
import cn.com.bonc.utils.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

/**
 * 故障待机判断处理类
 *
 * @author wzq
 * @date 2019-11-01
 **/
public class FaultStandbyHandle {
    
    /**
     * @param deviceDataMapCache   设备报警待机故障数据缓存
     * @param keys                 kafka中key拆分后的数组
     * @param fields               实时数据中fields对应的map表
     * @param faultStandbyRules    故障待机判断规则
     * @param faultStandbyDataList 故障待机数据集合
     */
    public static void core(Map<String, Object> deviceDataMapCache, String[] keys, Map<String, Object> fields, Tuple2<Set<String>, Map<String, Map<String, List<FaultStandbyRule>>>> faultStandbyRules, List<FaultStandbyData> faultStandbyDataList) {
        Set<String> prodCodes = faultStandbyRules.f0;
        Map<String, List<FaultStandbyRule>> faultRules = faultStandbyRules.f1.get("FAULT");
        Map<String, List<FaultStandbyRule>> standbyRules = faultStandbyRules.f1.get("STANDBY");
        Map.Entry<String, Object> entry = fields.entrySet().iterator().next();
        if (prodCodes.contains(keys[1]) && !entry.getKey().contains("V_DEVICE")) {
            //如果该数据对应的模板在判断规则之内，并且测点编码不包含“V_DEVICE”，则进行故障待机判断
            //故障判断，如果不符合故障规则，则进行待机判断
            if (faultRules != null && faultRules.containsKey(keys[1])) {
                ruleHandle(keys, fields, faultRules.get(keys[1]), faultStandbyDataList);
                if (faultStandbyDataList.size() == 0 && standbyRules != null && standbyRules.containsKey(keys[1])) {
                    ruleHandle(keys, fields, standbyRules.get(keys[1]), faultStandbyDataList);
                }
            } else {
                if (standbyRules != null && standbyRules.containsKey(keys[1])) {
                    ruleHandle(keys, fields, standbyRules.get(keys[1]), faultStandbyDataList);
                }
            }
        }
        String deviceCode = keys[2];
        boolean exists = deviceDataMapCache.containsKey(deviceCode);
        if (faultStandbyDataList.size() > 0) {
            if (exists) {
                Set<FaultStandbyData> removeFaultStandbyData = new HashSet<>();
                //如果redis中存在该设备数据，说明该设备之前就有问题，需要把之前的数据取出来和现在的数据作比较，如果是不同的点产生的问题，需要针对新点发送数据
                FaultStandbyData deviceFaultStandbyData = (FaultStandbyData) deviceDataMapCache.get(deviceCode);
                Set<String> propCodeSet = deviceFaultStandbyData.getPropCodeSet();
                for (FaultStandbyData data : faultStandbyDataList) {
                    //如果故障待机数据集合中有数据，则认为该设备现在有问题
                    if (propCodeSet.contains(data.getPropCode())) {
                        //如果之前Redis中保存的问题数据中包含了现在这个点编码，则需要将该数据加入到移除集合中
                        removeFaultStandbyData.add(data);
                    } else {
                        //如果之前redis中保存的问题数据中没有包含现在这个点编码，则需要将该点编码加入到点编码集合中
                        propCodeSet.add(data.getPropCode());
                    }
                }
                //根据Redis中设备数据的问题点集合删除当前设备触发的问题点
                for (FaultStandbyData data : removeFaultStandbyData) {
                    faultStandbyDataList.remove(data);
                }
                //将本次设备新增的问题点集合设置到Redis数据中
                deviceFaultStandbyData.setPropCodeSet(propCodeSet);
                deviceDataMapCache.put(deviceCode, deviceFaultStandbyData);
            } else {
                //如果redis中不存在该设备数据，说明该设备之前没有问题，需要发送数据，并将数据写到redis中
                Set<String> propCodeSet = new HashSet<>();
                for (FaultStandbyData data : faultStandbyDataList) {
                    propCodeSet.add(data.getPropCode());
                }
                //现在没有规定，因此直接去问题数据的第一个点（应该是随机的）直接作为问题点保存到Redis中，之后就不再更改。
                FaultStandbyData data = faultStandbyDataList.get(0);
                data.setPropCodeSet(propCodeSet);
                deviceDataMapCache.put(deviceCode, data);
            }
        } else {
            //如果故障待机数据集合中没有数据，则认为该设备现在没有问题
            if (exists) {
                //如果redis中存在该设备数据，说明该设备之前有问题，需要发送问题结束数据
                FaultStandbyData faultStandbyData = (FaultStandbyData) deviceDataMapCache.get(deviceCode);
                faultStandbyData.setEndTime(Long.parseLong(((Map<String, Object>) entry.getValue()).get("time").toString()));
                faultStandbyData.setTag(1);
                faultStandbyDataList.add(faultStandbyData);
                deviceDataMapCache.remove(deviceCode);
            }
        }
    }
    
    /**
     * 设备故障或状态判断，如果设备发生了问题，则组织问题数据，然后更新redis
     *
     * @param keys                 kafka中key拆分后的数组
     * @param fields               实时数据中fields对应的map表
     * @param faultStandbyRules    某个模板对应的故障待机判断规则
     * @param faultStandbyDataList 故障待机数据集合
     */
    private static void ruleHandle(String[] keys, Map<String, Object> fields, List<FaultStandbyRule> faultStandbyRules, List<FaultStandbyData> faultStandbyDataList) {
        long realTime;
        FaultStandbyData faultStandbyData;
        //遍历所有规则，进行规则判断
        for (FaultStandbyRule rule : faultStandbyRules) {
            Map<String, Object> thisPointData = (Map<String, Object>) fields.get(rule.propCode());
            if (thisPointData == null) {
                //如果该规则中的测点编码在实际数据中不存在，则结束该规则的判断
                break;
            }
            double realValue = Double.MIN_VALUE;
            String stringValue = thisPointData.get("value").toString();
            if (StringUtils.isNumber(stringValue)) {
                realValue = Double.parseDouble(thisPointData.get("value").toString());
            }
            realTime = Long.parseLong(thisPointData.get("time").toString());
            boolean isConformRule = false;
            if (rule.constantType()) {
                //常量判断
                if (judge(rule.judgeType(), realValue, rule.propValue())) {
                    //符合判断规则
                    isConformRule = true;
                }
            } else {
                //变量判断
                Map<String, Object> anotherPointData = (Map<String, Object>) fields.get(rule.anotherPropCode());
                double anotherRealValue = Double.MIN_VALUE;
                if (StringUtils.isNumber(anotherPointData.get("value").toString())) {
                    anotherRealValue = Double.parseDouble(anotherPointData.get("value").toString());
                }
                boolean conformJudgeType = judge(rule.judgeType(), realValue, anotherRealValue);
                if (rule.greaterThanZero()) {
                    //需要该点数值>0
                    if (realValue > 0 && conformJudgeType) {
                        //符合判断规则
                        isConformRule = true;
                    }
                } else {
                    //不需要该点数值>0
                    if (conformJudgeType) {
                        isConformRule = true;
                    }
                }
            }
            if (isConformRule) {
                //如果符合判断规则，则认为该数据符合判断依据，将其封装到结果集中
                HashSet<String> propCodeSet = new HashSet<>();
                propCodeSet.add(rule.propCode());
                faultStandbyData = new FaultStandbyData(0, keys[1], keys[2], rule.propCode(), propCodeSet, rule.propName(), rule.useType(), rule.rulePk(), stringValue, realTime, 0L);
                faultStandbyDataList.add(faultStandbyData);
            }
        }
    }
    
    
    /**
     * 判断数据是否符合判断规则
     *
     * @param judgeType 判断依据符号，只有" >、<、>=、<=、==、!="
     * @param realValue 测点实际数据
     * @param propValue 测点判断值
     * @return 是否符合。如果判断一句符号不在规定范围之内，则返回false
     */
    private static boolean judge(String judgeType, double realValue, double propValue) {
        if (Objects.equals(judgeType, ">")) {
            return realValue > propValue;
        } else if (Objects.equals(judgeType, "<")) {
            return realValue < propValue;
        } else if (Objects.equals(judgeType, ">=")) {
            return realValue >= propValue;
        } else if (Objects.equals(judgeType, "<=")) {
            return realValue <= propValue;
        } else if (Objects.equals(judgeType, "=")) {
            return realValue == propValue;
        } else if (Objects.equals(judgeType, "!=")) {
            return realValue != propValue;
        } else {
            throw new RuntimeException("判断依据符号不在规定范围之内：" + judgeType);
        }
    }
    
}
