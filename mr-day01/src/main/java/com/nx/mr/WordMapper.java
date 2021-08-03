package com.nx.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN:是指框架读取到的数据集的key的类型，在默认的情况下，读取到的key是一行的数据相对于文本的开头的偏移量
 *     VALUEIN:是指框架读取到数据集的value的类型。在默认的情况下，读取到的value就是一行的数据
 *     KEYOUT:是指用户自定义业务方法中返回数据中的key的类型，由用户根据业务逻辑自己决定的，在当前的wordcount中，这个key就是单词
 *     VALUEOUT:是指用户自定义业务方法中返回的数据中的value的类型，由用户根据业务逻辑自己定义。在当前的wordcount中，这个value就是key的次数
 *
 *     Long和String类型是jdk中的数据类型，在序列化的时候，效率低(这些类型序列化涉及到继承关系，其实hadoop并不需要)
 *     hadoop为了提供效率，自定义了一套序列化框架
 *     在hadoop的程序中，如果要进行序列化（写磁盘，网络传输数据等等），一定要使用hadoop的实现的序列化的类型
 *     Long --》 LongWritable
 *     String --》 Text
 *     Integer --》IntWritable
 *     Null --》NullWritable
 * @Date:Created in 21:29 2021/8/1
 * @Modified By:
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /**
     *
     * @param key  就是偏移量
     * @param value  就是一行文本
     * @param context  上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //业务逻辑
        //1.切分单词
        String[] words = value.toString().split(" ");
        //2.计数一次，帮单词转化成类似<hello,1>这样的key-value的类型
        for (String word : words) {
            //3.写入上下文
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
