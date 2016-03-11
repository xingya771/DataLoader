package com.ojdbc.util;

/**
 * Created by Arthur on 2016/3/2.
 */
/**
 *
 */


import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.File;
import java.sql.*;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * 获取数据库连接
 *
 * @author Shengxingya 2013-11-15 下午1:50:07
 */
public class DBUtil {
    /**
     * 日志
     */
    private static Logger logger = Logger.getLogger(DBUtil.class);
    private static DataSource dataSource;
    private static String dateFormat = Parameters.$("datafile.dateFormat");
    private static final String sql_insert = "insert into #name(#cols) values(#values)";
    private static final String SQL_GET_COLS_TYPE_ORACLE = "select COLUMN_NAME,DATA_TYPE from user_tab_cols where TABLE_NAME='";

    public static Connection getConn() {
        if (dataSource == null) {
            init();
        }
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void init() {
        try {
            dataSource = BasicDataSourceFactory.createDataSource(Parameters.properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定表的所有列及列类型
     *
     * @param tName        表名
     * @param databaseType 数据库类型
     * @return key=列名,value=类型的键值对
     * @throws SQLException
     */
    public static Map<String, String> getColsType(String tName, String databaseType) throws SQLException {
        switch (databaseType.toLowerCase()) {
            case "oracle":
                return getColsType_oracle(tName);
            case "mysql":
            default:
                return getColsType_oracle(tName);
        }
    }

    /**
     * 获取oracle中指定表的所有列及列类型
     *
     * @param tName 表名
     * @return key=列名,value=类型的键值对
     * @throws SQLException
     */
    public static Map<String, String> getColsType_oracle(String tName) throws SQLException {

        Map<String, String> map = new HashMap<>();
        String sql = SQL_GET_COLS_TYPE_ORACLE + tName.toUpperCase() + "'";
        Connection connection = getConn();
        Statement st = connection.createStatement();
        ResultSet rs = null;

        try {
            rs = st.executeQuery(sql);
            map = new HashMap<String, String>();
            while (rs.next()) {
                map.put(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE"));

            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return map;
    }

    /**
     * 根据列数据类型设置PreparedStatement
     *
     * @param type      列类型
     * @param ps        PreparedStatement对象
     * @param i
     * @param colValues
     * @throws SQLException
     * @throws ParseException
     */
    public static void setPSValues(String type, PreparedStatement ps, int i, String colValues) throws SQLException, ParseException {
        switch (type) {
            case "CHAR":
            case "VARCHAR":
            case "VARCHAR2":
                ps.setString(i + 1, colValues);
                break;
            case "NUMBER":
                ps.setDouble(i + 1, Double.parseDouble(colValues));
                break;
            case "INTEGER":
                ps.setInt(i + 1, Integer.parseInt(colValues));
                break;
            case "DATE":
                ps.setDate(i + 1, new Date(DateUtils.parseDate(colValues, dateFormat).getTime()));
                break;
            default:
                ps.setString(i + 1, colValues);
        }
    }

    public static String createPreparedStatementSql(String tName, String title[], String split) {

        int titleColsCounts = title.length;
        StringBuilder titleCols=new StringBuilder();
       

        StringBuilder questionMark = new StringBuilder("");
        for (int i = 0; i < titleColsCounts; i++) {
            questionMark.append("?");
            titleCols.append(title[i]);
            if (i != titleColsCounts - 1) {
                questionMark.append(",");
                titleCols.append(",");
            }
        }
        return sql_insert.replace("#name", tName).replace("#cols", titleCols).replace("#values", questionMark);
    }

    /**
     * 从文件名得到表名，需要文件名的前缀与表名一致
     *
     * @param fileName 文件名
     * @return 大写的表名
     */
    public static String getTableNameFromFileName(String fileName) {
        return fileName.substring(0, fileName.indexOf(".")).toUpperCase();
    }

    /**
     * 从文件对象得到表名，需要文件名的前缀与表名一致
     *
     * @param file 文件
     * @return 大写的表名
     */
    public static String getTableNameFromFileName(File file) {
        return getTableNameFromFileName(file.getName());
    }

    public static int getBatchCommitSuccessCount(int res[]) {
        int s = 0;
        for (int i : res) {
            if (i != Statement.EXECUTE_FAILED) {
                s += 1;
            }
        }
        return s;
    }
}
