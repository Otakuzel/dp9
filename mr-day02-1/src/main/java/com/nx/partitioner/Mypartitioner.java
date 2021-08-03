package com.nx.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author:zel
 * @Description:
 * <KEY, VALUE>
 * @Date:Created in 19:43 2021/8/3
 * @Modified By:
 */
public class Mypartitioner extends Partitioner <Text, LongWritable>{
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        if (text.toString().length() >=5){
            return 0;
        }else{
            return 1;
        }
    }
}
