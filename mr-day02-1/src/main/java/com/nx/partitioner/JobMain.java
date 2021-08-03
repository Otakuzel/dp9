package com.nx.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.LossyRetryInvocationHandler;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @Author:zel
 * @Description:
 * @Date:Created in 19:53 2021/8/3
 * @Modified By:
 */
public class JobMain {
    public static void main(String[] args) throws IOException {
        //一.初始化一个Job
        Configuration conf = new Configuration;
        Job job = Job.getInstance(conf,"partitioner");

        //二.设置Job的相关信息 8个小步骤
        //1.设置输入路径
        job.setInputFormatClass();
    }
}
