package com.yaya.util;


import com.google.common.base.CaseFormat;
import com.yaya.bean.TableProcessDim;
import com.yaya.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        //1.加载驱动
        Class.forName(Constant.MYSQL_DRIVER) ;
        //2.获取连接
        Connection connection = DriverManager.getConnection(
                Constant.MYSQL_URL,
                Constant.MYSQL_USER_NAME,
                Constant.MYSQL_PASSWORD
        );

        return connection ;
    }

    public static void closeConnection(Connection connection) throws SQLException {
        if(connection !=null && !connection.isClosed()){
            connection.close();
        }
    }


    /**
     *  通用查询方法
     *
     */
    public static <T> List<T> queryList(Connection connection , String sql , Class<T> cls  , Boolean isUnderScoreToCamel) throws Exception {
        ArrayList<T> result = new ArrayList<>();

        // 3.编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        // 4.执行SQL
        ResultSet resultSet = preparedStatement.executeQuery();

        // 5.获取元数据
        ResultSetMetaData metaData = resultSet.getMetaData();
        //  获取结果列的个数
        int columnCount = metaData.getColumnCount();
        // 5.处理结果集， 封装对象
        while(resultSet.next()){
            //创建对象，用于封装结果
            T t = cls.newInstance();
            //处理每行数据
            for (int i = 1; i <= columnCount; i++) {
                //获取列名
                String columnName = metaData.getColumnLabel(i);
                //获取列值
                Object columnValue = resultSet.getObject(columnName);

                //字段名
                String fieldName = columnName ;

                if(isUnderScoreToCamel ){
                    // 下划线转驼峰
                    // source_table => sourceTable
                    fieldName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

//                封装到对象中
//                 fieldName( sourceTable )  => setSourceTable
//                Field field = cls.getDeclaredField("sourceTable");
//                Method method = cls.getDeclaredMethod("setSourceTable", field.getType());
//                method.invoke( t , columnValue) ;

                BeanUtils.copyProperty(t , fieldName , columnValue);

            }
            //添加到集合中
            result.add(t) ;
        }

        return result ;
    }

    public static void main(String[] args) throws Exception {
        Connection connection = getConnection();

        String sql = " select `source_table` , `sink_table` , `sink_family` , `sink_columns`, `sink_row_key` from gmall2024_config_230821.table_process_dim" ;

        List<TableProcessDim> tableProcessDimList = queryList(connection, sql, TableProcessDim.class, true);
        tableProcessDimList.forEach(System.out::println);

        closeConnection(connection);
    }
}
