package com.yaya.function;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.yaya.constant.Constant;
import com.yaya.util.HBaseUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.yaya.util.RedisUtil;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;


public abstract  class AsyncMapDimFunction<T> extends RichAsyncFunction<T , T> implements DimFunction<T> {

    AsyncConnection  hbaseAsyncConnection ;
    StatefulRedisConnection<String,String> redisAsyncConnection ;

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取Hbase的异步连接
        hbaseAsyncConnection = HBaseUtil.getAsyncConnection();
        //获取redis的异步连接
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection() ;
    }

    @Override
    public void close() throws Exception {
        //关闭连接
        HBaseUtil.closeAsyncConnection( hbaseAsyncConnection );
        RedisUtil.closeRedisAsyncConnection( redisAsyncConnection );
    }

    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {

        // 使用JDK8提供的异步编程框架

        CompletableFuture.supplyAsync(
                new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 1. 读取缓存中的维度数据
                        JSONObject redisDimObj
                                = RedisUtil.readDimAsync(redisAsyncConnection, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));
                        return redisDimObj ;
                    }
                }
        ).thenApplyAsync(
                new Function<JSONObject, JSONObject>() {
                    @Override
                    public @Nullable JSONObject apply(@Nullable JSONObject redisDimObj) {
                        JSONObject dimObj  = null ;
                        if(redisDimObj == null ){

                            try {
                                // 2. 读取Hbase中的维度数据
                                JSONObject hbaseDimObj =
                                        HBaseUtil.getAsyncCells(hbaseAsyncConnection, Constant.HBASE_NAMESPACE, getTableName(), getRowKey(bean));

                                //将维度数据设置到redis缓存中
                                RedisUtil.writeDimAsync(redisAsyncConnection , Constant.HBASE_NAMESPACE , getTableName() , getRowKey( bean ) , hbaseDimObj );

                                dimObj = hbaseDimObj ;

                            } catch (Exception e) {
                            }
                        }else{
                            dimObj = redisDimObj ;
                        }
                        return dimObj ;
                    }
                }
        ).thenAccept(
                new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dimObj) {
                        // 3. 维度关联
                        addDim( bean , dimObj );

                        // 4. 将结果写出
                        resultFuture.complete(Collections.singleton( bean ));
                    }
                }
        ) ;

    }

}
