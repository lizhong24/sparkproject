package com.wolf.sparkproject.dao;

import com.wolf.sparkproject.domain.SessionDetail;

/**
 * session明细接口
 */
public interface ISessionDetailDAO {
    /**
     * 插入一条session明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);
}
