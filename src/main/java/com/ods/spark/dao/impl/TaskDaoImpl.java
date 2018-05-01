package com.ods.spark.dao.impl;



import com.ods.spark.dao.ITaskDao;
import com.ods.spark.domain.Task;
import com.ods.spark.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDaoImpl implements ITaskDao {
    @Override
    public Task findById(long taskId) {
       final Task task=new Task();
        JDBCHelper jh=JDBCHelper.getInstance();
        String sql="select * from task where task_id=?";
        Object[] params=new Object[]{taskId};
        jh.executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    long taskId=rs.getLong(1);
                    String taskName=rs.getString(2);
                    String createTime=rs.getString(3);
                    String startTime=rs.getString(4);
                    String finishTime=rs.getString(5);
                    String taskType=rs.getString(6);
                    String taskStatus=rs.getString(7);
                    String taskParam=rs.getString(8);

                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });
              return task;
    }
}
