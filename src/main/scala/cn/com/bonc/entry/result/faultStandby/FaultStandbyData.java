package cn.com.bonc.entry.result.faultStandby;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Set;

/**
 * @author wzq
 * @date 2019-11-04
 **/
public class FaultStandbyData {
    /**
     * 数据标识：0表示异常，1表示正常
     */
    private int tag;
    /**
     * 模板编码
     */
    private String prodCode;
    /**
     * 设备编码
     */
    private String deviceCode;
    /**
     * 测点编码
     */
    private String propCode;
    /**
     * 测点编码集合，用来记录已经发送过数据的测点编码
     */
    private Set<String> propCodeSet;
    /**
     * 测点名称
     */
    private String propName;
    /**
     * 故障待机状态判断 FAULTSTART/FAULTEND/STANDBY
     */
    private String useType;
    /**
     * 规则主键 _PK
     */
    private String rulePk;
    /**
     * 接收到的真实值
     */
    private String value;
    /**
     * 开始时间
     */
    private Long startTime;
    /**
     * 结束时间
     */
    private Long endTime;
    
    public FaultStandbyData() {
    }
    
    public FaultStandbyData(int tag, String prodCode, String deviceCode, String propCode, Set<String> propCodeSet, String propName, String useType, String rulePk, String value, Long startTime, Long endTime) {
        this.tag = tag;
        this.prodCode = prodCode;
        this.deviceCode = deviceCode;
        this.propCode = propCode;
        this.propCodeSet = propCodeSet;
        this.propName = propName;
        this.useType = useType;
        this.rulePk = rulePk;
        this.value = value;
        this.startTime = startTime;
        this.endTime = endTime;
    }
    
    public Set<String> getPropCodeSet() {
        return propCodeSet;
    }
    
    public void setPropCodeSet(Set<String> propCodeSet) {
        this.propCodeSet = propCodeSet;
    }
    
    public int getTag() {
        return tag;
    }
    
    public void setTag(int tag) {
        this.tag = tag;
    }
    
    public String getProdCode() {
        return prodCode;
    }
    
    public void setProdCode(String prodCode) {
        this.prodCode = prodCode;
    }
    
    public String getDeviceCode() {
        return deviceCode;
    }
    
    public void setDeviceCode(String deviceCode) {
        this.deviceCode = deviceCode;
    }
    
    public String getPropCode() {
        return propCode;
    }
    
    public void setPropCode(String propCode) {
        this.propCode = propCode;
    }
    
    public String getPropName() {
        return propName;
    }
    
    public void setPropName(String propName) {
        this.propName = propName;
    }
    
    public String getUseType() {
        return useType;
    }
    
    public void setUseType(String useType) {
        this.useType = useType;
    }
    
    public String getRulePk() {
        return rulePk;
    }
    
    public void setRulePk(String rulePk) {
        this.rulePk = rulePk;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public Long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
    
    public Long getEndTime() {
        return endTime;
    }
    
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (!(o instanceof FaultStandbyData)) {
            return false;
        }
        
        FaultStandbyData data = (FaultStandbyData) o;
        
        return new EqualsBuilder()
                .append(getTag(), data.getTag())
                .append(getProdCode(), data.getProdCode())
                .append(getDeviceCode(), data.getDeviceCode())
                .append(getPropCode(), data.getPropCode())
                .append(getPropCodeSet(), data.getPropCodeSet())
                .append(getPropName(), data.getPropName())
                .append(getUseType(), data.getUseType())
                .append(getRulePk(), data.getRulePk())
                .append(getValue(), data.getValue())
                .append(getStartTime(), data.getStartTime())
                .append(getEndTime(), data.getEndTime())
                .isEquals();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getTag())
                .append(getProdCode())
                .append(getDeviceCode())
                .append(getPropCode())
                .append(getPropCodeSet())
                .append(getPropName())
                .append(getUseType())
                .append(getRulePk())
                .append(getValue())
                .append(getStartTime())
                .append(getEndTime())
                .toHashCode();
    }
    
    @Override
    public String toString() {
        return "FaultStandbyData{" +
                "tag=" + tag +
                ", prodCode='" + prodCode + '\'' +
                ", deviceCode='" + deviceCode + '\'' +
                ", propCode='" + propCode + '\'' +
                ", propName='" + propName + '\'' +
                ", useType='" + useType + '\'' +
                ", rulePk='" + rulePk + '\'' +
                ", value=" + value +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
