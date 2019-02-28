package com.wolf.sparkproject.dao.factory;

import com.wolf.sparkproject.dao.*;
import com.wolf.sparkproject.impl.*;

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

    /**
     * top10品类DAO
     * @return ITop10CategoryDAO
     */
    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }
}
