package com.jacle.hive.jdbc.utils;

import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.xml.internal.security.Init;
import org.apache.directory.shared.kerberos.codec.apReq.actions.StoreTicket;
import org.codehaus.jettison.json.JSONString;

import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Description:
 * author:Jacle
 * Date:2018/9/4
 * Time:11:04
 **/
public class HiveConnUtil
{
    private static String drivername = "org.apache.hive.jdbc.HiveDriver";
    private static Properties ps = new Properties();
    private static String hiveUrl;
    private static String username;
    private static String password;

    //初始化jdbc的hive连接
    static
    {
        try
        {
            Class.forName(drivername);
            ps.load(HiveConnUtil.class.getClassLoader().getResourceAsStream("./hive.properties"));
            hiveUrl = ps.getProperty("hive.url");
            username = ps.getProperty("hive.username");
            password = ps.getProperty("hive.password");
        } catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 获取单个的hive连接
     *
     * @return connection
     */
    public static Connection getConn()
    {
        try
        {
            Connection connection = DriverManager.getConnection(hiveUrl, username, password);
            return connection;
        } catch (SQLException e)
        {
            e.printStackTrace();
        }

        return null;
    }


    /**
     * 获取查询的结果，返回json格式
     * @param sql
     * @param params
     * @return
     */
    public static String getDataOfJson(String sql, String[] params)
    {
        Connection connection = getConn();
        String resultJson = "";
        PreparedStatement ps=null;
        ResultSet rt=null;
        List<HashMap<String,String>> list = new ArrayList<HashMap<String, String>>();

        try
        {
           ps= connection.prepareStatement(sql);
           if(params!=null)
           {
               //设置参数
               for(int i=0;i<params.length;i++)
               {
                   ps.setString(i+1, params[i]);
               }
           }

           //开始查询
           rt=ps.executeQuery();

           ResultSetMetaData metaData=null;
           while(rt.next())
           {
               metaData=rt.getMetaData();
               HashMap<String,String> map = new HashMap<>();
               for (int i=0;i<metaData.getColumnCount();i++)
               {
                   map.put(metaData.getColumnName(i+1), rt.getString(metaData.getColumnName(i+1)));
               }

               list.add(map);
           }

           //将数据转换为json格式
            resultJson=JSONObject.toJSONString(list);

        } catch (SQLException e)
        {
            e.printStackTrace();
        }finally
        {
            closeConn(connection, ps, rt);
        }
        return resultJson;
    }


    /**
     * 执行无返回值的sql查询
     * @param sql
     * @param params
     * @return
     */
    public static boolean executeSql(String sql, String[] params)
    {
        Connection connection = getConn();
        PreparedStatement ps=null;
        boolean flag=false;

        try
        {
            ps= connection.prepareStatement(sql);
            if(params!=null)
            {
                //设置参数
                for(int i=0;i<params.length;i++)
                {
                    ps.setString(i+1, params[i]);
                }
            }

            //开始执行无返回值的查询
            flag=ps.execute();

        } catch (SQLException e)
        {
            e.printStackTrace();
        }finally
        {
            closeConn(connection, ps, null);
        }
        return flag;
    }


    /**
     * 关闭资源
     * attention:注意关闭的顺序，rt->ps->conn
     * @param connection
     * @param statement
     * @param resultSet
     */
    public static void closeConn(Connection connection, Statement statement, ResultSet resultSet)
    {

        if (resultSet != null)
        {
            try
            {
                resultSet.close();
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }

        if (statement != null)
        {
            try
            {
                statement.close();
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }


        if (connection != null)
        {
            try
            {
                connection.close();
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        }
    }
}
