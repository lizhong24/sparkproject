package com.wolf.sparkproject.constant;

/**
 * 常量接口
 */
public interface Constants {
    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String SPARK_LOCAL = "spark.local";

    /**
     * spark作业相关的常量
     */
    String SPARK_APP_NAME = "UserVisitSessionAnalyzeSpark";
    
}
