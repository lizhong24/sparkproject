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
}
