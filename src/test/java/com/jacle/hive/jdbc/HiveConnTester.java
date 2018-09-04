package com.jacle.hive.jdbc;

import com.jacle.hive.jdbc.utils.HiveConnUtil;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/4
 * Time:14:15
 **/
public class HiveConnTester
{
    private Logger logger=Logger.getLogger(HiveConnTester.class);

    @Test
    public void testExecuteQuery() throws  Exception
    {
        Connection connection = HiveConnUtil.getConn();

        Statement statement = connection.createStatement();
        //设置参数，优化hive的执行
        boolean flag=statement.execute("set hive.execution.engine=tez");
        if(flag)
        {
            logger.info("setting tez works");
        }else
        {
            logger.info("error setting tez");
        }

        ResultSet rt=statement.executeQuery("desc t_fund");
        while(rt.next())
        {
            System.out.println(rt.getString(1));
        }

        rt.close();
        connection.close();
    }
}
