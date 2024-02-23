package com.yaya.function;

import com.alibaba.fastjson.JSONObject;
import com.yaya.constant.Constant;
import com.yaya.util.HBaseUtil;
import com.yaya.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public abstract class MapDimFunction<T> extends RichMapFunction<T,T> implements DimFunction<T>{

    //初始化连接提升全局
    Connection connection;
    Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化 Hbase Redis 连接
        connection = HBaseUtil.getConnection();
        jedis = RedisUtil.getJedis() ;
    }

    @Override
    public void close() throws Exception {
        //关闭连接
        HBaseUtil.closeConnection(connection);
        RedisUtil.closeJedis(jedis);
    }

    @Override
    public T map(T bean) throws Exception {
        //先尝试从 redis 获取数据
        JSONObject dimObj = RedisUtil.readDim(jedis, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));
        //判断是否获取到数据 没有从Hbase获取数据 在将数据写入Redis作为缓存
        if(dimObj == null ){
            //Hbase获取数据
            dimObj = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));
            //存入到redis中
            RedisUtil.writeDim(jedis , Constant.HBASE_NAMESPACE, getTableName() , getRowKey(bean) , dimObj  );

            System.out.println("测试打印-------->数据在 Hbase 读取: " + Constant.HBASE_NAMESPACE + ":"+ getTableName() +":" +  getRowKey(bean));
        }else{
            System.out.println("测试打印-------->数据在 Redis 读取: " + Constant.HBASE_NAMESPACE + ":"+ getTableName()+":" +  getRowKey(bean));
        }

        addDim( bean , dimObj  );

        return bean ;
    }



}
