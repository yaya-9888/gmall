package com.yaya.function;

import com.yaya.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitWordFunction extends TableFunction<Row> {

    public void eval(String searchValue) {
        //先分词
        Set<String> words = IkUtil.splitWord(searchValue);
        for (String word : words) {
            collect( Row.of( word ));
        }
    }
}
