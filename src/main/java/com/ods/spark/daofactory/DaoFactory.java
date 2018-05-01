package com.ods.spark.daofactory;


import com.ods.spark.dao.IAdBlacklistDAO;
import com.ods.spark.dao.IAreaTop3ProductDAO;
import com.ods.spark.dao.ITaskDao;
import com.ods.spark.dao.impl.AdBlacklistDAOImpl;
import com.ods.spark.dao.impl.AreaTop3ProductDAOImpl;
import com.ods.spark.dao.impl.TaskDaoImpl;

public class DaoFactory {
    public static ITaskDao getTaskDao(){
        return new TaskDaoImpl();

    }
    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }
    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }
 /*   public static ISessionAggStatDao getSessionAggStatDao(){
        return new SessionAggStatDaoImpl();
    }
    public static ISessionRandomExtractDao getSessionRandomExtractDao(){
          return new SessionRandomExtractDaoImpl();
    }
    public static ISessionDetailDao getSessionDetailDao(){
         return new SessionDetailDaoImpl();
    }
    public static ITop10CategoryDao getTop10CategoryDao(){
         return new Top10CategoryDaoImpl();
    }
    public static IAreaTop3ProductDAO getAreaTop3ProductDAO(){
        return new AreaTop3ProductDaoImpl();
    }

    public static IAdUserClickCountDao getAdUserClickCountDao(){
        return new AdUserClickCountDaoImpl();
    }
    public static IAdBlackListDao getAdBlackListDao(){
        return new AdBlackListDaoImpl();
    }
    */
}
