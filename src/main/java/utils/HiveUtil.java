package utils;

import java.sql.*;

public class HiveUtil {

    private static final String hiveDriver;
    private static final String hiveUrl;
    private static final String hiveUsername;
    private static final String hivePassword;

    static {
        hiveDriver = "org.apache.hadoop.hive.jdbc.HiveDriver";
        hiveUrl = "jdbc:hive://119.254.161.52:10001/woss_hive";
        hiveUsername = "";
        hivePassword = "";
    }


    // 创建hive connection
    public static Connection getHiveConn() {
        Connection conn = null;
        try {
            Class.forName(hiveDriver);
            conn = DriverManager.getConnection(hiveUrl, hiveUsername, hivePassword);
            System.out.println("get connection success");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            // System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            // System.exit(1);
        }
        return conn;
    }

    public static void close(ResultSet rs, Statement stmt, Connection con) {
        try {
            if (rs != null) rs.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            if (stmt != null) stmt.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            if (con != null) con.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
