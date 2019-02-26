package com.wolf.sparkproject.dao.factory;

import com.wolf.sparkproject.dao.ITaskDAO;
import com.wolf.sparkproject.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {
    /**
     * 获取任务管理DAO
     */
    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }
}
