package com.wolf.sparkproject.impl;

import com.wolf.sparkproject.dao.ISessionDetailDAO;
import com.wolf.sparkproject.domain.SessionDetail;
import com.wolf.sparkproject.jdbc.JDBCHelper;

/**
 * session明细DAO实现类
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO{
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] params = new Object[] {
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
