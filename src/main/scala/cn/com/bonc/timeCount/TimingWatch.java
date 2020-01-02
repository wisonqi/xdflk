package cn.com.bonc.timeCount;

import java.util.Map;
import java.util.TreeMap;

/**
 * 用于记录代码执行时间点，以用于分析代码执行的时间
 *
 * @author wzq
 * @date 2019-10-30
 **/
public class TimingWatch {
    
    private Map<Integer, Long> recordMap;
    
    /**
     * 创建一个treeMap，对key自然排序
     */
    public TimingWatch() {
        recordMap = new TreeMap<>();
    }
    
    /**
     * 记录新的时间点，注意不能使用同一个数字进行重复记录，必须从数字1开始，然后递增即可<br>
     * 为方便观察，可以在代码块之间使用两个连续的数字，而在另外的代码块之间使用另外两个连续的数字
     *
     * @param i 新纪录数字标识
     */
    public void record(int i) {
        if (recordMap.containsKey(i)) {
            throw new RuntimeException("时间点记录键：" + i + "已存在。");
        } else {
            recordMap.put(i, System.currentTimeMillis());
        }
    }
    
    /**
     * 获取各个代码块执行时间<br>
     * 该方法只会组织两个相近的数字之间的代码块执行时间，两数字之间相差为1。
     *
     * @return 组织好的字符串，可以直接输出
     */
    public String getWatchMessage() {
        if (recordMap.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        int lastKey = 1;
        long lastValue = recordMap.get(1);
        recordMap.remove(1);
        for (Map.Entry<Integer, Long> entry : recordMap.entrySet()) {
            int currentKey = entry.getKey();
            long currentValue = entry.getValue();
            if (currentKey - lastKey == 1) {
                sb.append(currentKey - 1).append("~").append(currentKey).append(":").append(currentValue - lastValue).append(", ");
            }
            lastKey = currentKey;
            lastValue = currentValue;
        }
        return sb.deleteCharAt(sb.length() - 1).toString();
    }
    
    /**
     * 根据描述信息组织各个代码块执行时间并返回
     *
     * @param description 描述信息，key为 “数字~数字”，value为具体的描述
     * @return 组织好的字符串，可以直接输出
     */
    public String getWatchMessage(Map<String, String> description) {
        String result = getWatchMessage();
        for (Map.Entry<String, String> entry : description.entrySet()) {
            result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
}
