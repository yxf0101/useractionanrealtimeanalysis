package com.ods.spark.dao;


import com.ods.spark.domain.Task;

public interface ITaskDao {
    Task findById(long taskId);
}
