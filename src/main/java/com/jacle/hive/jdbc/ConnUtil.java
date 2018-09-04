package com.jacle.hive.jdbc;

import java.sql.*;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/3
 * Time:17:44
 **/
public class ConnUtil
{
    public static Connection getConn()
    {
        try
        {
            //hive server1
//            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
            //hive server2
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection conn=DriverManager.getConnection("jdbc:hive2://10.101.0.151:10000/db_fund","","");
            System.out.println(conn);
            return conn;
        } catch (SQLException e)
        {
            e.printStackTrace();
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) throws  Exception
    {
        Connection conn=getConn();
        Statement statement = conn.createStatement();

        //查询指定无参数的sql
        ResultSet rt=statement.executeQuery("select count(*) cout from t_fund");
        rt.next();
        System.out.println(rt.getString("cout"));
        rt.close();

        PreparedStatement preparedStatement = conn.prepareStatement("select * from t_fund where innercode=?");
        preparedStatement.setInt(1,2441 );
        rt=preparedStatement.executeQuery();
        while(rt.next())
        {
            ResultSetMetaData metaData=rt.getMetaData();
            for(int i=0;i<metaData.getColumnCount();i++)
            {
                System.out.println(rt.getString(metaData.getColumnLabel(i+1)));
            }
        }

        rt.close();
        conn.close();;

    }
}
