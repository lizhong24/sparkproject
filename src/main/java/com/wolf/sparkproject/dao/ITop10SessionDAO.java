package com.wolf.sparkproject.dao;

import com.wolf.sparkproject.domain.Top10Session;

/**
 * top10活跃session的DAO接口
 */
public interface ITop10SessionDAO {
    void insert(Top10Session top10Session);
}