package com.wolf.sparkproject.dao.factory;

import com.wolf.sparkproject.dao.ISessionAggrStatDAO;
import com.wolf.sparkproject.dao.ISessionDetailDAO;
import com.wolf.sparkproject.dao.ISessionRandomExtractDAO;
import com.wolf.sparkproject.dao.ITaskDAO;
import com.wolf.sparkproject.impl.SessionAggrStatDAOImpl;
import com.wolf.sparkproject.impl.SessionDetailDAOImpl;
import com.wolf.sparkproject.impl.SessionRandomExtractDAOImpl;
import com.wolf.sparkproject.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {
    /**
     * 获取任务管理DAO
     * @return ITaskDAO
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

    /**
     * session随机抽取模块DAO
     * @return ISessionRandomExtractDAO
     */
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    /**
     * session明细DAO
     * @return ISessionDetailDAO
     */
    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }
}
