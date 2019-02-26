package com.wolf.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 */
public class ConfigurationManager {
	private static Properties prop = new Properties();
	
	/**
	 * 静态代码块
	 */
	static{
		try {
			InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
     * 获取指定key对应的value
     * @param key
     * @return
     */
    public static String getProperty (String key) {
        return prop.getProperty(key);
    }
    
    /**
     * 获取Integer类型的配置项
     * @param key
     * @return
     */
    public static Integer getInteger (String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;		
	}
}
