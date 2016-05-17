package com.taomee.bigdata.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.IOException;
import com.taomee.bigdata.lib.ReturnCode;

public class MysqlConnection
{
    private Connection mysqlConn = null;
    private String mysqlUrl = null;
    private String mysqlUser = null;
    private String mysqlPasswd = null;
    private ReturnCode code = ReturnCode.get();

    public MysqlConnection() { }

    public boolean connect(String url, String user, String passwd) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            code.setCode("E_MYSQL_LIB", "no MySQL library found!");
            System.err.println("no MySQL library found!");
            return false;
        }
        if(url == null || user == null || passwd == null) {
            code.setCode("E_MYSQL_PARAM", String.format("missing mysql configure : mysqlUrl[%s], mysqlUser[%s], mysqlPasswd[%s]", url, user, passwd));
            System.err.println(String.format("missing mysql configure : mysqlUrl[%s], mysqlUser[%s], mysqlPasswd[%s]", url, user, passwd));
            return false;
        }
        try {
            mysqlConn = DriverManager.getConnection(url, user, passwd);
        } catch (SQLException e) {
            code.setCode("E_MYSQL_CONNECT", e.getMessage());
            System.err.println(e.getMessage());
            return false;
        }
        mysqlUrl = new String(url);
        mysqlUser = new String(user);
        mysqlPasswd = new String(passwd);
        return true;
    }

    public void finalize() {
        close();
    }

    public void close() {
        if(mysqlConn != null) {
            try {
                mysqlConn.close();
            } catch(SQLException e) {
                code.setCode("E_MYSQL_CLOSE", e.getMessage());
            }
        }
        mysqlConn = null;
    }

    public ResultSet doSql(String sql) {
        if(sql == null || sql.trim().length() == 0) {
            code.setCode("E_SQL_EMPTY", "sql is empty");
            return null;
        }
        ResultSet result = null;
        try {
            mysqlConn.clearWarnings();
            result = mysqlConn.createStatement().executeQuery(sql);
        } catch(SQLException e) {
            code.setCode("E_DO_SQL", String.format("[%s] %s", sql, e.getMessage()));
            return null;
        }
        return result;
    }

    public int doUpdate(String sql) {
        if(sql == null || sql.trim().length() == 0) {
            code.setCode("E_SQL_EMPTY", "sql is empty");
            return -1;
        }
        try { 
            mysqlConn.clearWarnings();
            return mysqlConn.createStatement().executeUpdate(sql);
        } catch(SQLException e) {
            code.setCode("E_DO_UPDATE", String.format("[%s] %s", sql, e.getMessage()));
            return -1;
        }
    }
}
