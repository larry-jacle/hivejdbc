package com.jacle.hive.jdbc;

import com.jacle.hive.jdbc.utils.HiveConnUtil;
import org.junit.Test;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/4
 * Time:14:13
 **/
public class ConnTester
{
    @Test
    public void testConn()
    {
        System.out.println(HiveConnUtil.getConn());
    }

    @Test
    public void testQuery()
    {
        String resultJson=HiveConnUtil.getDataOfJson("select * from t_fund limit 10", null);
        System.out.println(resultJson);
    }
}
