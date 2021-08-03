package com.nx.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN:map阶段输出的key类型
 *     VALUEIN:map阶段输出的value类型
 *     KEYOUT:最终结果的key的类型，在本案例中就是单词的类型。Text
 *     VALUEOUT:最终结果的value的类型，在本案例中就是次数的类型.LongWritable
 *     如果其中对应的map的key比较多(比如hello),mr是基于磁盘跑，无论数据多大，reduce都会基于磁盘一个一个去跑。不同于spark基于内存去跑，spark可能会出现溢出
 * @Date:Created in 22:04 2021/8/1
 * @Modified By:
 */
public class WordReducer extends Reducer <Text, LongWritable, Text, LongWritable>{
    /**
     * 具体业务逻辑
     * @param key  单词
     * @param values   相同单词的次数
     * @param context  上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义一个变量
        long count = 0;
        //2、迭代累加
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key,new LongWritable(count));
        //3、写入到上下文
    }
}
