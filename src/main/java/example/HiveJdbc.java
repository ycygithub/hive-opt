package example;

import utils.HiveUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chongyu on 10/30/14.
 */
public class HiveJdbc {

    public static void main(String[] args) throws Exception {

        Connection connection = null;
        Statement stmt = null;
        ResultSet res = null;

        List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();

        String dbName = "recommenddb";
        String hql = "select * from user_click_sup_d limit 10";

        try {
            connection = HiveUtil.getHiveConn();
            stmt = connection.createStatement();
            boolean status = stmt.execute("use " + dbName);
            if (status) {
                res = stmt.executeQuery(hql);
                while (res.next()) {
                    ResultSetMetaData metaData = res.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    Map<String,Object> oneData = new HashMap<String,Object>();
                    for(int i=0;i<columnCount;i++){
                        String columnName = metaData.getColumnName(i + 1);
                        Object columnData = res.getObject( i + 1);
                        oneData.put(columnName,columnData);
                    }
                    result.add(oneData);
                }
            }
        } finally {
            System.out.println(result);
            HiveUtil.close(res, stmt, connection);
        }

    }

}
