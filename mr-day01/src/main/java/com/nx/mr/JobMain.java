package com.nx.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * 主类，将Mapper和Reducer两个阶段串联起来，提供程序运行接口
 * @Date:Created in 22:19 2021/8/1
 * @Modified By:
 */
public class JobMain {
    /**
     * 程序入口
     * 其中用一个Job类对象管理程序运行的很多参数
     * 指定用哪个类作为mapper的业务逻辑类，指定哪个类作为Reducer的业务逻辑类
     * .......
     * 其他各种需要的参数
     * @param args
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "wordcount");

        //二、设置Job对象的相关的信息，里面包含了8个小步骤
        //1、设置输入路径，让程序要找到源文件的位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("D://zel/MrTest/WordCount/test1.txt"));
        //TextInputFormat.addInputPath(job,new Path("hdfs://192.168.22.128:8020/wordcount.txt"));

        //2、设置Mapper的类型，并设置k2  v2
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //3 4 5 6 四个步骤， Shuffle阶段，现在使用默认的就可以。
        //7、设置Reducer类型，并设置 k3 v3
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //8、设置输出路径，让结果数据放到某个地方去
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("D://zel/MrTest/WordCount/word_out"));
        //TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.22.128:8020/woud_out0728"));
        //三、等待程序完成（任务的提交）
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
        System.exit(b ? 0 : 1);

    }
}
