package com.wolf.sparkproject.test;

import com.wolf.sparkproject.dao.ITaskDAO;
import com.wolf.sparkproject.dao.factory.DAOFactory;
import com.wolf.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 *
 * INSERT INTO `sparkproject`.`task` (`task_id`, `task_name`) VALUES ('2', '测试任务01');
 */
public class TaskDAOTest {
    public static void main(String[] args) {
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(2);
        System.out.println(task.getTaskName());
    }
}
