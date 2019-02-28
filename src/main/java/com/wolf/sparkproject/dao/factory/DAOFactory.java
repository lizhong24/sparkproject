package com.wolf.sparkproject.dao.factory;

import com.wolf.sparkproject.dao.ISessionAggrStatDAO;
import com.wolf.sparkproject.dao.ISessionRandomExtractDAO;
import com.wolf.sparkproject.dao.ITaskDAO;
import com.wolf.sparkproject.impl.SessionAggrStatDAOImpl;
import com.wolf.sparkproject.impl.SessionRandomExtractDAOImpl;
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

    /**
     * 获取session聚合统计DAO
     * @return ISessionAggrStatDAO
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }
}
