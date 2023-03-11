package org.duo.udf;

import java.sql.*;

/**
 * @Auther:duo
 * @Date: 2023-02-14 - 02 - 14 - 13:13
 * @Description: org.duo.udf
 * @Version: 1.0
 */
public class HiveJdbcClient {

    public static final String dirverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {

        try {
            Class.forName(dirverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Connection conn = DriverManager.getConnection("jdbc:hive2://server01:10000/default", "root", "123456");

        Statement statement = conn.createStatement();
        String sql = "select * from logtbl";
        ResultSet resultSet = statement.executeQuery(sql);

        while(resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }
}
