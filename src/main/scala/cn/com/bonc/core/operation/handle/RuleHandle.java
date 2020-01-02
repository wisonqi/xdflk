package cn.com.bonc.core.operation.handle;


import cn.com.bonc.entry.result.HandleResult;
import cn.com.bonc.entry.result.faultStandby.FaultStandbyData;
import cn.com.bonc.entry.result.warning.TimeWarn;
import cn.com.bonc.entry.result.warning.ValueWarn;
import cn.com.bonc.entry.result.warning.WarningData;
import cn.com.bonc.entry.rule.faultStandby.FaultStandbyRule;
import cn.com.bonc.entry.rule.warning.Rule;
import cn.com.bonc.entry.rule.warning.TimeRule;
import cn.com.bonc.entry.rule.warning.ValueRule;
import cn.com.bonc.utils.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import scala.collection.Iterator;
import scala.collection.mutable.ListBuffer;

import java.util.*;

/**
 * 实时json数据规则处理<br>
 * 包含测点名称处理、报警判断处理、故障待机判断处理
 *
 * @author wzq
 * @date 2019年9月6日15:29:31
 **/
public class RuleHandle {
    
    /**
     * 根据测点名称数据给实时数据添加测点名称信息，key为：pointName
     *
     * @param pointInfoMap 测点名称存储表
     * @param key          实时数据key
     * @param data         实时数据值
     * @return 添加了测点名称的实时数据
     */
    public static JSONObject pointNameHandle(Map<String, String> pointInfoMap, String key, String data) {
        String templateCode = key.split("/")[1];
        JSONObject object = JSON.parseObject(data);
        JSONObject fields = object.getJSONObject("fields");
        for (String key1 : fields.keySet()) {
            fields.getJSONObject(key1).put("pointName", pointInfoMap.getOrDefault(templateCode + "~" + key1, "未知"));
        }
        return object;
    }
    
    /**
     * 根据报警规则和故障待机规则处理实时数据
     *
     * @param deviceDataMapCache 设备报警待机故障数据缓存
     * @param rules              报警规则存储表
     * @param keys               kafka实时数据key，按照 / 分割之后分别为租户、模板编码、设备编码
     * @param data               kafka实时数据value
     * @param faultStandbyRules  故障待机判断规则对象
     * @return 处理之后包含全量数据和报警数据的结果对象
     */
    public static HandleResult ruleHandle(Map<String, Object> deviceDataMapCache, Map<String, Rule> rules, String[] keys, JSONObject data, Tuple2<Set<String>, Map<String, Map<String, List<FaultStandbyRule>>>> faultStandbyRules) {
        ArrayList<WarningData> warningDatas = new ArrayList<>();
        //实时数据map表，最外层map，用来保存所有数据，最终转换为json字符串
        Map<String, Object> fullDataMap = new HashMap<>(16);
        //手动处理实时数据
        fullDataMap.put("prodCode", keys[1]);
        fullDataMap.put("deviceCode", keys[2]);
        fullDataMap.put("gateway", data.get("gateway"));
        //存放测点数据，最终保存至fullDataMap的fields对应的value值
        Map<String, Object> fullFieldsMap = new HashMap<>(16);
        Map<String, Object> fields = data.getJSONObject("fields");
        //处理所有测点的时间和数值以及故障待机判断，主要处理方法
        List<FaultStandbyData> faultStandbyDataList = new ArrayList<>();
        String originalString = JSON.toJSONString(deviceDataMapCache);
        dataHandle(deviceDataMapCache, rules, keys, fields, fullFieldsMap, warningDatas, faultStandbyRules, faultStandbyDataList);
        String currentString = JSON.toJSONString(deviceDataMapCache);
        Map<String, String> deviceDataChangeMap = null;
        if (!Objects.equals(originalString, currentString)) {
            //如果设备缓存数据在规则处理之前和规则处理之后有变化，则需要将变化之后的新数据写入redis
            deviceDataChangeMap = new HashMap<>(16);
            deviceDataChangeMap.put(keys[2], currentString);
        }
        fullDataMap.put("pointInfo", fullFieldsMap);
        ArrayList<String> warningDataJsonList = new ArrayList<>();
        for (WarningData warningData : warningDatas) {
            if (warningData.tag() == 2) {
                HashSet<String> warnCodeSet = warningData.valueWarn().warnCodeSet();
                for (String warnCode : warnCodeSet) {
                    warningData.valueWarn().warnCode_$eq(warnCode);
                    warningDataJsonList.add(JSON.toJSONString(warningData));
                }
            } else {
                warningDataJsonList.add(JSON.toJSONString(warningData));
            }
        }
        return dataTypeHandle(fullDataMap, warningDataJsonList, faultStandbyDataList, deviceDataChangeMap);
    }
    
    /**
     * 处理所有测点的时间和数值，使用到redis
     *
     * @param deviceDataMapCache   设备报警待机故障数据缓存
     * @param rules                报警规则存储表
     * @param keys                 kafka中key拆分后的数组
     * @param fields               实时数据中fields对应的map表
     * @param fieldsMap            返回全量数据的fields对应的map表
     * @param warningDatas         报警数据集合
     * @param standbyFaultRules    待机故障判断规则
     * @param faultStandbyDataList 故障待机数据集合
     */
    private static void dataHandle(Map<String, Object> deviceDataMapCache, Map<String, Rule> rules, String[] keys, Map<String, Object> fields, Map<String, Object> fieldsMap, ArrayList<WarningData> warningDatas, Tuple2<Set<String>, Map<String, Map<String, List<FaultStandbyRule>>>> standbyFaultRules, List<FaultStandbyData> faultStandbyDataList) {
        //遍历fields，该map表中存放的是所有测点编码及其对应的时间和值
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String pointCode = entry.getKey();
            String templateDevicePoint = keys[1] + "~" + keys[2] + "~" + pointCode;
            String templatePoint = keys[1] + "~~" + pointCode;
            Map<String, Object> pointData = (Map<String, Object>) entry.getValue();
            String pointName = pointData.get("pointName").toString();
            long realTime = Long.parseLong(pointData.get("time").toString());
            double realValue;
            String stringValue = pointData.get("value").toString();
            if (StringUtils.isNumber(stringValue)) {
                stringValue = StringUtils.truncate(stringValue);
                realValue = Double.parseDouble(stringValue);
            } else {
                //如果value值为true，转换为1，否则全部转换为0
                if (Objects.equals(stringValue, "true")) {
                    realValue = 1;
                } else {
                    realValue = 0;
                }
            }
            
            //组织模板、设备、测点编码信息，查找对应规则
            Rule rule = rules.get(templateDevicePoint) != null ? rules.get(templateDevicePoint) : rules.get(templatePoint);
            if (rule != null) {
                //如果找到规则，则进行判断
                ListBuffer<TimeRule> timeRuleListBuffer = rule.timeRules();
                Iterator<ValueRule> valueRulesIterator = rule.valueRules().iterator();
                boolean isWarning = false;
                ValueRule valueRule;
                while (valueRulesIterator.hasNext()) {
                    valueRule = valueRulesIterator.next();
                    Double min = valueRule.value_min();
                    Double max = valueRule.value_max();
                    /*
                        默认上下和下限至少一个有值
                        值报警规则都是互斥的，只要满足一个，就可以认为是报警，然后处理组织报警数据和全量数据即可，处理完结束迭代
                     */
                    isWarning =
                            (min != null && max == null && realValue <= min) ||
                                    (min != null && max != null && realValue >= min && realValue < max) ||
                                    (min == null && max != null && realValue >= max);
                    if (isWarning) {
                        //如果该点为计算型数据，则不需要走redis
                        if (!pointCode.contains("V_DEVICE")) {
                            valueWarningHandleWarn(deviceDataMapCache, rule, pointName, valueRule, timeRuleListBuffer, (templateDevicePoint), stringValue, realTime, warningDatas);
                        }
                        valueWarningHandleFull(pointCode, pointName, valueRule, rule, realTime, stringValue, fieldsMap);
                        break;
                    }
                }
                //找到了报警规则，但是并没有报警的正常数据处理
                if (!isWarning) {
                    noWarningHandle(deviceDataMapCache, pointCode, pointName, templateDevicePoint, realTime, stringValue, rule, fieldsMap, warningDatas);
                }
            } else {
                //如果没有找到规则，则认为是正常数据
                noRuleHandle(deviceDataMapCache, pointCode, templateDevicePoint, pointName, realTime, stringValue, fieldsMap, warningDatas);
            }
        }
        //对故障待机数据的判断处理
        FaultStandbyHandle.core(deviceDataMapCache, keys, fields, standbyFaultRules, faultStandbyDataList);
    }
    
    /**
     * 找到报警规则，对于单条实时正常数据的处理<br>
     * 包括全量数据和Redis中报警数据的处理
     *
     * @param warningDataMapCache 报警数据缓存
     * @param pointCode           测点编码
     * @param pointName           测点名称
     * @param templateDevicePoint 模板、设备、测点编码组合
     * @param realTime            测点实时数据时间
     * @param stringValue         测定实时数据数值
     * @param rule                报警规则
     * @param fieldsMap           返回全量数据的fields对应的map表
     * @param warningDatas        报警数据集合
     */
    private static void noWarningHandle(Map<String, Object> warningDataMapCache, String pointCode, String pointName, String templateDevicePoint, long realTime, String stringValue, Rule rule, Map<String, Object> fieldsMap, ArrayList<WarningData> warningDatas) {
        //1. 处理全量数据
        Map<String, Object> pointData = new HashMap<>(8);
        pointData.put("tag", 1);
        pointData.put("time", realTime);
        pointData.put("value", stringValue);
        pointData.put("pointName", pointName);
        pointData.put("alarmType", "C001");
        pointData.put("isShow", rule.isShow());
        fieldsMap.put(pointCode, pointData);
        //如果该点为计算型数据，则不需要走redis
        if (!pointCode.contains("V_DEVICE")) {
            //2. 处理报警数据
            if (warningDataMapCache.containsKey(templateDevicePoint)) {
                //如果Redis中有报警数据，则认为这个点报警已经结束，需要删除Redis数据，然后组织一条有结束时间的报警数据封装到结果对象中；否则认为上次也没有报警，什么都不做
                WarningData warningData = (WarningData) warningDataMapCache.get(templateDevicePoint);
                warningData.tag_$eq(2);
                warningData.pointName_$eq(pointName);
                warningData.isWarnPoint_$eq(rule.isWarnPoint());
                warningData.value_$eq(stringValue);
                warningData.warnTime_$eq(realTime - warningData.startTime());
                warningData.endTime_$eq(realTime);
                warningDatas.add(warningData);
                warningDataMapCache.remove(templateDevicePoint);
            }
        }
        
    }
    
    /**
     * 没有找到报警规则，对于单条实时正常数据的处理<br>
     * 包括全量数据和Redis中报警数据的处理
     *
     * @param warningDataMapCache 报警数据缓存
     * @param pointCode           测点编码
     * @param templateDevicePoint 模板、设备、测点编码组合
     * @param pointName           测点名称
     * @param time                测点实时数据时间
     * @param stringValue         测定实时数据数值
     * @param fieldsMap           返回全量数据的fields对应的map表
     * @param warningDatas        报警数据集合
     */
    private static void noRuleHandle(Map<String, Object> warningDataMapCache, String pointCode, String templateDevicePoint, String pointName, long time, String stringValue, Map<String, Object> fieldsMap, ArrayList<WarningData> warningDatas) {
        //1. 处理全量数据
        Map<String, Object> pointData = new HashMap<>(8);
        pointData.put("tag", 1);
        pointData.put("time", time);
        pointData.put("value", stringValue);
        pointData.put("pointName", pointName);
        pointData.put("alarmType", "C001");
        pointData.put("isShow", "1");
        fieldsMap.put(pointCode, pointData);
        //如果该点为计算型数据，则不需要走redis
        if (!pointCode.contains("V_DEVICE")) {
            //2. 处理报警数据
            if (warningDataMapCache.containsKey(templateDevicePoint)) {
                //如果Redis中有报警数据，则认为这个点报警已经结束，需要删除Redis数据，然后组织一条有结束时间的报警数据封装到结果对象中；否则认为上次也没有报警，什么都不做
                WarningData warningData = (WarningData) warningDataMapCache.get(templateDevicePoint);
                warningData.tag_$eq(2);
                warningData.warnTime_$eq(time - warningData.startTime());
                warningData.endTime_$eq(time);
                warningDatas.add(warningData);
                warningDataMapCache.remove(templateDevicePoint);
            }
        }
    }
    
    /**
     * 处理单条值报警类型数据，这个方法针对于一条实时数据为值报警情况下全量数据的处理<br>
     * 只包含全量数据的处理
     *
     * @param pointCode   测点编码
     * @param pointName   测点名称
     * @param valueRule   值类型报警信息
     * @param rule        报警信息
     * @param time        kafka实时数据中的时间
     * @param stringValue kafka实时数据中的原数值
     * @param fieldsMap   返回全量数据的fields对应的map表
     */
    private static void valueWarningHandleFull(String pointCode, String pointName, ValueRule valueRule, Rule rule, long time, String stringValue, Map<String, Object> fieldsMap) {
        Map<String, Object> pointData = new HashMap<>(8);
        pointData.put("tag", 0);
        pointData.put("time", time);
        pointData.put("value", stringValue);
        pointData.put("pointName", pointName);
        pointData.put("alarmType", valueRule.warnStyle());
        pointData.put("isShow", rule.isShow());
        fieldsMap.put(pointCode, pointData);
    }
    
    /**
     * 处理单条值报警类型数据，这个方法指针对于一条实时数据为值报警情况下报警数据的处理<br>
     * 包括Redis和报警数据的处理
     *
     * @param warningDataMapCache 报警数据缓存
     * @param rule                报警规则
     * @param pointName           测点名称
     * @param valueRule           值类型报警信息
     * @param timeRuleListBuffer  时长类型报警集合
     * @param templateDevicePoint 模板、设备、测点编码组合
     * @param stringValue         kafka实时数据中的数值
     * @param realTime            kafka实时数据中的时间
     * @param warningDatas        报警数据集合
     */
    private static void valueWarningHandleWarn(Map<String, Object> warningDataMapCache, Rule rule, String pointName, ValueRule valueRule, ListBuffer<TimeRule> timeRuleListBuffer, String templateDevicePoint, String stringValue, long realTime, ArrayList<WarningData> warningDatas) {
        //由于时长规则判断需要用到两个挨着的报警时长，因此需要将集合中的时长规则按序取出来放到数组中，方便后面的判断
        int timeRulesLength = timeRuleListBuffer.size();
        TimeRule[] timeRules = new TimeRule[timeRulesLength];
        Iterator<TimeRule> iterator = timeRuleListBuffer.iterator();
        for (int index = 0; index < timeRulesLength; index++) {
            timeRules[index] = iterator.next();
        }
        ValueWarn currentValueWarn = new ValueWarn();
        currentValueWarn.warnCode_$eq(valueRule.warnCode());
        currentValueWarn.warnCodeSet().add(valueRule.warnCode());
        currentValueWarn.warnStyle_$eq(valueRule.warnStyle());
        currentValueWarn.sendToPeople_$eq(valueRule.sendToPeople());
        currentValueWarn.sendToRole_$eq(valueRule.sendRole());
        currentValueWarn.sendToDept_$eq(valueRule.sendToDept());
        WarningData warningData;
        if (warningDataMapCache.containsKey(templateDevicePoint)) {
            //如果Redis中有这个测点的报警数据，则说明这个测点之前报过警，则需要判断是否符合时长报警
            warningData = (WarningData) warningDataMapCache.get(templateDevicePoint);
            long startTime = warningData.startTime();
            long duration = realTime - startTime;
            int index;
            boolean isTimeWarning = false;
            //遍历时长规则，看时间差是否在相近两个时间之间或大于最后一个时间
            for (index = 0; index < timeRulesLength; index++) {
                int index1 = index + 1;
                if (index1 < timeRulesLength) {
                    //如果下一个时长规则对象索引小于数组长度，则说明还有下一个时长规则对象，现在应该判断这次报警数据和最开始报警数据的时差是否在这两个时长报警范围之内
                    long minDuration = timeRules[index].warnTime();
                    long maxDuration = timeRules[index1].warnTime();
                    if (duration < minDuration) {
                        break;
                    } else if (duration < maxDuration) {
                        isTimeWarning = true;
                        break;
                    }
                } else {
                    //如果下一个时长规则对象索引不小于数组长度，说明这是最后一个时长规则对象，现在应该判断这次报警数据和最开始报警数据的时差是否不小于这个时长报警时长
                    if (duration >= timeRules[index].warnTime()) {
                        isTimeWarning = true;
                        break;
                    }
                }
            }
            if (isTimeWarning) {
                TimeRule timeRule = timeRules[index];
                //如果符合时长报警，则取出对应时长报警规则的相关数据，组织报警数据，封装到结果对象中
                //如果不符合时长报警，则认为这个报警数据之前已经发送过值报警数据，现在不用做任何事
                String currentWarnCode = timeRule.warnTimeCode();
                TimeWarn timeWarn = warningData.timeWarn();
                if (timeWarn != null) {
                    //如果Redis中数据有时长报警，则需要比较处理
                    if (!Objects.equals(currentWarnCode, timeWarn.warnTimeCode())) {
                        //如果两个时长报警编码不一致，则说明这次是新的时长类型报警，需要组织报警数据重新设置到Redis中并并封装到结果对象中；否则什么都不做
                        generateWarningData(warningDataMapCache, rule, pointName, templateDevicePoint, stringValue, realTime, warningDatas, warningData, timeRule, currentValueWarn);
                    }
                } else {
                    //如果Redis中数据没有时长报警，则认为第一次触发了时长报警，需要组织报警数据，更新Redis，并封装到结果对象中
                    generateWarningData(warningDataMapCache, rule, pointName, templateDevicePoint, stringValue, realTime, warningDatas, warningData, timeRule, currentValueWarn);
                }
            }
            /*
                看是不是不同的值报警
                如果是不同的值报警，则需要查看当前值报警是否已经存在于报警编码集合中
                    如果不存在，则需要组织报警数据放到Redis中，并封装到操作结果对象中
                    否则什么都不做
             */
            if (!warningData.valueWarn().warnCodeSet().contains(valueRule.warnCode())) {
                //将原始数据中的所有值报警编码全部取出来放到新的值报警数据中
                currentValueWarn.warnCodeSet().addAll(warningData.valueWarn().warnCodeSet());
                warningData.valueWarn_$eq(currentValueWarn);
                //如果要发送不同的值报警，则需要发送当前报警数据的信息，包括开始时间、值
                warningData.setStartTime(realTime);
                warningData.setValue(stringValue);
                warningDataMapCache.put(templateDevicePoint, warningData);
                warningDatas.add(warningData);
            }
        } else {
            //如果Redis中不存在这个测点的报警数据，则认为是第一次报警，需要组织报警数据放到Redis中，并封装到操作结果对象中
            String[] keys = templateDevicePoint.split("~");
            warningData = new WarningData();
            warningData.tag_$eq(0);
            warningData.templateCode_$eq(keys[0]);
            warningData.deviceCode_$eq(keys[1]);
            warningData.pointCode_$eq(keys[2]);
            warningData.pointName_$eq(pointName);
            warningData.isWarnPoint_$eq(rule.isWarnPoint());
            warningData.warnCode_$eq(rule.warnCode());
            warningData.value_$eq(stringValue);
            warningData.valueWarn_$eq(currentValueWarn);
            warningData.startTime_$eq(realTime);
            warningDataMapCache.put(templateDevicePoint, warningData);
            warningDatas.add(warningData);
        }
    }
    
    /**
     * 组织触发了时长报警的报警数据
     *
     * @param warningDataMapCache 报警数据缓存
     * @param rule                报警规则
     * @param pointName           测点名称
     * @param templateDevicePoint 模板、设备、点编码
     * @param stringValue         实时数据点位数值
     * @param realTime            实时数据点位时间
     * @param warningDatas        报警数据集合
     * @param warningData         报警数据对象
     * @param timeRule            触发的时间报警规则
     * @param valueWarn           值报警数据
     */
    private static void generateWarningData(Map<String, Object> warningDataMapCache, Rule rule, String pointName, String templateDevicePoint, String stringValue, long realTime, ArrayList<WarningData> warningDatas, WarningData warningData, TimeRule timeRule, ValueWarn valueWarn) {
        //每次组织报警信息，都需要重新设置所有属性，以使用最新报警规则中的最新数据
        warningData.tag_$eq(1);
        warningData.pointName_$eq(pointName);
        warningData.isWarnPoint_$eq(rule.isWarnPoint());
        warningData.value_$eq(stringValue);
        warningData.endTime_$eq(realTime);
        warningData.warnTime_$eq(realTime - warningData.startTime());
        TimeWarn currentTimeWarn = new TimeWarn();
        currentTimeWarn.warnTimeCode_$eq(timeRule.warnTimeCode());
        currentTimeWarn.sendToPeople_$eq(timeRule.sendToPeople());
        currentTimeWarn.setSendToRole(timeRule.sendToRole());
        currentTimeWarn.sendToCaptain_$eq(timeRule.sendToCaptain());
        currentTimeWarn.sendToDept_$eq(timeRule.sendToDept());
        warningData.valueWarn_$eq(valueWarn);
        warningData.timeWarn_$eq(currentTimeWarn);
        //由于该方法调用之前，已经经过了计算点和实际点的判断，因此这儿无需判断是否为实际点
        warningDataMapCache.put(templateDevicePoint, warningData);
        warningDatas.add(warningData);
    }
    
    
    /**
     * 数据类型判断处理
     *
     * @param fullDataMap          处理完的实时数据
     * @param warningData          处理完的报警数据
     * @param faultStandbyDataList 故障待机数据集合
     * @param deviceDataChangeMap  设备数据缓存更改之后的数据，如果是删除，则只需要将value值设置为空字符串即可
     * @return 处理结果对象
     */
    private static HandleResult dataTypeHandle(Map<String, Object> fullDataMap, ArrayList<String> warningData, List<FaultStandbyData> faultStandbyDataList, Map<String, String> deviceDataChangeMap) {
        String tag = "";
        Map<String, Object> result = new HashMap<>(16);
        Map<String, Object> pointInfo = (Map<String, Object>) fullDataMap.get("pointInfo");
        for (Map.Entry<String, Object> entry : pointInfo.entrySet()) {
            String key = entry.getKey();
            Map<String, Object> point = (Map<String, Object>) entry.getValue();
            if (key.startsWith("V_DEVICE_P")) {
                tag = "A";
                result.put("prodCode", fullDataMap.get("prodCode"));
                result.put("deviceCode", fullDataMap.get("deviceCode"));
                result.put("gateway", fullDataMap.get("gateway"));
                if (Objects.equals(point.get("alarmType").toString(), "C001")) {
                    result.put("deviceAlarmType", "A002");
                } else {
                    result.put("deviceAlarmType", point.get("alarmType"));
                }
                //由于A类数据和B类数据只有一个值，因此只需要判断第一个测点编码就可以识别出该数据属于哪类数据
                break;
            } else if (key.startsWith("V_DEVICE_L")) {
                tag = "B";
                break;
            }
        }
        Set<String> faultStandbyDataSet = new HashSet<>();
        for (FaultStandbyData data : faultStandbyDataList) {
            faultStandbyDataSet.add(JSON.toJSONString(data));
        }
        if (Objects.equals("A", tag)) {
            return new HandleResult(tag, JSON.toJSONString(result), warningData, faultStandbyDataSet, deviceDataChangeMap);
        } else if (Objects.equals("B", tag)) {
            return new HandleResult(tag, JSON.toJSONString(fullDataMap), warningData, faultStandbyDataSet, deviceDataChangeMap);
        } else {
            return new HandleResult("C", JSON.toJSONString(fullDataMap), warningData, faultStandbyDataSet, deviceDataChangeMap);
        }
    }
    
    
}
