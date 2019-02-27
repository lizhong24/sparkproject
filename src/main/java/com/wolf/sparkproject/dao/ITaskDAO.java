package com.wolf.sparkproject.dao;

import com.wolf.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 */
public interface ITaskDAO {
    /**
     * 根据主键查询业务
     * @param taskid 任务id
     * @return 任务
     */
    Task findById(long taskid);
}
