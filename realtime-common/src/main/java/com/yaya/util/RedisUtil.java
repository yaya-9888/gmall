package com.yaya.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yaya.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public class RedisUtil {

    private static JedisPool jedisPool ;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();

        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        jedisPool = new JedisPool( config , Constant.REDIS_HOST , Constant.REDIS_PORT) ;
    }

    /**
     * 获取Jedis连接
     * @return
     */
    public static Jedis getJedis(){

        return jedisPool.getResource() ;
    }

    /**
     * 关闭Jedis连接
     * @param jedis
     */
    public static void closeJedis(Jedis jedis){
        if(jedis != null) {
            jedis.close();
        }
    }

    /**
     * 关闭 redis 的异步连接
     *
     * @param redisAsyncConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }



    /**
     * 获取到 redis 的异步连接
     *
     * @return 异步链接对象
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient =
                RedisClient.create("redis://" + Constant.REDIS_HOST+ ":" + Constant.REDIS_PORT + "/0");
        return redisClient.connect();
    }













    /**
     * 获取redis中的key
     */
    public static String getKey(String namespaceName , String tableName , String rowkey ){
        return namespaceName +":" + tableName + ":" + rowkey ;
    }

    /**
     * 将Hbase中的维度数据写入到Redis中
     *
     * type:   string
     * key :   namespaceName:tableName:rowkey
     * value:  jsonstr
     * 写入api: setex
     * 读取api: get
     * 是否过期: 1天
     */
    public static void writeDim( Jedis jedis , String  namespaceName , String tableName , String rowkey  , JSONObject dim  ){
        String key  = getKey(namespaceName, tableName, rowkey);
        jedis.setex( key ,  Constant.ONE_DAY_SECONDS , dim.toJSONString()) ;
    }


    /**
     * 从Redis中读取维度数据
     */
    public static JSONObject readDim(Jedis jedis , String  namespaceName , String tableName , String rowkey  ){
        String key  = getKey(namespaceName, tableName, rowkey);
        String dimStr = jedis.get(key);
        if(dimStr != null ){
            return JSONObject.parseObject( dimStr ) ;
        }

        return null ;
    }

    /**
     * 从Redis中删除对应的维度数据
     */

    public static void deleteDim(Jedis jedis , String  namespaceName , String tableName , String rowkey ) {
        String key  = getKey(namespaceName, tableName, rowkey);
        jedis.del( key ) ;
    }


    /**
     * 把维度异步的写入到 redis 中
     * @param redisAsyncConn  到 redis 的异步连接
     * @param tableName 表名
     * @param id id 的值
     * @param dim 要写入的维度数据
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                     String namespaceName,
                                     String tableName,
                                     String id,
                                     JSONObject dim) {
        // 1. 得到异步命令
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();

        String key = getKey( namespaceName , tableName, id);
        // 2. 写入到 string 中: 顺便还设置的 ttl
        asyncCommand.setex(key, Constant.ONE_DAY_SECONDS, dim.toJSONString());

    }

    /**
     * 异步的方式从 redis 读取维度数据
     * @param redisAsyncConn 异步连接
     * @param tableName 表名
     * @param id id 的值
     * @return 读取到维度数据,封装的 json 对象中
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                          String namespaceName ,
                                          String tableName,
                                          String id) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(namespaceName , tableName, id);
        try {
            String json = asyncCommand.get(key).get();
            if (json != null) {
                return JSON.parseObject(json);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }





}
