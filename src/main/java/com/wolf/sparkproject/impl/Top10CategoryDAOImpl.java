package com.wolf.sparkproject.impl;

import com.wolf.sparkproject.dao.ITop10CategoryDAO;
import com.wolf.sparkproject.domain.Top10Category;
import com.wolf.sparkproject.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

    public void insert(Top10Category category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[]{
                category.getTaskid(),
                category.getCategoryid(),
                category.getClickCount(),
                category.getOrderCount(),
                category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
