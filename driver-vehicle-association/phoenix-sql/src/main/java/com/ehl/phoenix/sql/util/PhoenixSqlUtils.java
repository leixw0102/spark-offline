package com.ehl.phoenix.sql.util;

import com.ehl.offline.common.EhlConfiguration;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * Created by 雷晓武 on 2017/3/8.
 */
public class PhoenixSqlUtils {
    private EhlConfiguration conf;

    private static PhoenixSqlUtils phoenixSqlUtils;
    public PhoenixSqlUtils(){
        this("phoenix.conf");
    }
    private String driver;
    private String url;
    private String user;
    private String password;
    public PhoenixSqlUtils(String file){
        conf = new EhlConfiguration().addResource(file);
        driver=conf.get("phoneix.jdbc.driver");
        url = conf.get("phoneix.jdbc.url");
        user = conf.get("phoneix.jdbc.user","");
        password = conf.get("phoneix.jdbc.password","");
    }

    public synchronized static PhoenixSqlUtils getInstance(){
        if(phoenixSqlUtils==null){
           phoenixSqlUtils = new PhoenixSqlUtils();
        }
        return phoenixSqlUtils;
    }

    public synchronized static PhoenixSqlUtils getInstance(String conf){
        if(phoenixSqlUtils == null){
            phoenixSqlUtils = new PhoenixSqlUtils(conf);
        }
        return phoenixSqlUtils;
    }

    public Connection getConnection() throws Exception {
        try{
            Class.forName(driver);
            return DriverManager.getConnection(url,user,password);
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new Exception("get connection error");
    }

    public void close(Connection conn) throws SQLException {
        if(null != conn){
            conn.close();
        }
    }

    public <T> T execute(Callback<T> callback) throws Exception {
        Connection con =null;
        try{
           con =  getConnection();
           return callback.call(con);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            close(con);
        }
        throw new Exception("execute callback error");
    }

}

