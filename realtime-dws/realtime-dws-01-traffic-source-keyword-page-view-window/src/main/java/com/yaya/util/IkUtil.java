package com.yaya.util;

import org.jline.utils.Log;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

public class IkUtil {

    public static Set<String> splitWord(String searchValue ){
        HashSet<String> result = new HashSet<>();
        StringReader reader = new StringReader(searchValue);
        //Ik 分词处理
        IKSegmenter segmenter = new IKSegmenter(reader , true  );
        try {
            Lexeme next = segmenter.next();
            while(next != null ){
                String word = next.getLexemeText();
                result.add(word) ;
                //取下个词
                next = segmenter.next() ;
            }
        }catch (Exception e){
            Log.error("分词失败 :" + searchValue);
        }
        return result ;
    }
}
