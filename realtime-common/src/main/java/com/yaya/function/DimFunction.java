package com.yaya.function;

import com.alibaba.fastjson.JSONObject;


public interface DimFunction<T> {

   String getTableName() ;
   String getRowKey(T bean ) ;
   void addDim(T bean , JSONObject dimObj);
}
