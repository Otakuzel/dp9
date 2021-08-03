package com.nx.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * @Date:Created in 19:33 2021/8/3
 * @Modified By:
 */
public class WordMapper extends Mapper<LongWritable,Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
