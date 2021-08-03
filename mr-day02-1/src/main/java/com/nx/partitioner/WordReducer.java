package com.nx.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * @Date:Created in 19:40 2021/8/3
 * @Modified By:
 */
public class WordReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key,new LongWritable(count));
    }
}
