package cn.com.bonc.utils;

import java.io.*;

/**
 * 对象序列化和反序列化工具类
 *
 * @author wzq
 * @date 2019-09-06
 **/
public class SerializeUtils {
    
    /**
     * 对象序列化为byte数组
     *
     * @param o 待序列化对象
     * @return byte数组
     */
    public static byte[] serialize(Object o) {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            bytes = baos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }
    
    /**
     * byte数组反序列化为对象
     *
     * @param bytes 待反序列化byte数组
     * @return 反序列化之后的对象
     */
    public static Object deserialize(byte[] bytes) {
        Object o = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            o = ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return o;
    }
    
}
