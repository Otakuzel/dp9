package com.nx.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.LossyRetryInvocationHandler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * @Date:Created in 19:53 2021/8/3
 * @Modified By:
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一.初始化一个Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"partitioner");

        //二.设置Job的相关信息 8个小步骤
        //1.设置输入路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D://zel/MrTest/WordCount/test1.txt"));
        //2.设置Mapper类型，并设置k2,v2
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3 4 5 6Shuffle阶段
        //3.设置分区
        job.setPartitionerClass(Mypartitioner.class);
        //7.设置Reduce类型
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置ReduceTask的个数
        job.setNumReduceTasks(2);
        //8.设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D://zel/MrTest/WordCount/word_out"));
        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);
    }
}
