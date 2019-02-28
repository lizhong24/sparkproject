package com.wolf.sparkproject.dao;

import com.wolf.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 */
public interface ISessionRandomExtractDAO {
    /**
     * 插入session随机抽取
     */
    void insert(SessionRandomExtract sessionRandomExtract);
}
