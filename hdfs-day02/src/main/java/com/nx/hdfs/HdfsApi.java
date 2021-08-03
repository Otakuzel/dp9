package com.nx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsApi {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        HdfsApi hdfsApi = new HdfsApi();
//        hdfsApi.getFileSystem();
//        hdfsApi.listFiles();
//        hdfsApi.mkdirs();
        hdfsApi.uploadFile();
    }
    //方式一：FileSystem
    public void getFileSystem() throws IOException {
        //1.创建Configuration对象
        Configuration conf = new Configuration();
        //2.获取文件系统的类型
        conf.set("fs.defaultFS","hdfs://hadoop0:8020");
        //3.获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(conf);
        //4.打印输出测试结果
        System.out.println(fileSystem);
        }
    /**
     * 方式二：set方式+通过newInstance
     * @throws IOException
     */
    public void getFileSystem2() throws IOException {
        //1:创建Configuration对象
        Configuration conf = new Configuration();

        //2:设置文件系统类型
        conf.set("fs.defaultFS", "hdfs://hadoop0:8020");

        //3:获取指定文件系统
        FileSystem fileSystem = FileSystem.newInstance(conf);

        //4:输出测试
        System.out.println(fileSystem);
    }

    /**
     * 方式三：new URI+get
     * @throws Exception
     */
    public void getFileSystem3() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }

    /**
     * 方式四：newInstance+get
     * @throws Exception
     */
    public void getFileSystem4() throws Exception{
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop0:8020"), new Configuration());
        System.out.println("fileSystem:"+fileSystem);
    }
    //文件的遍历
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        //1.获取FileSystems实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(),"root");
        //2.调用一个方法
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        //3.迭代遍历
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus.getPath() + "====" + fileStatus.getPath().getName());

        }
    }
    //创建文件夹
    public void mkdirs() throws URISyntaxException, IOException, InterruptedException {
        //1.获取FileSystems实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(),"root");
        //2.创建文件夹
        fileSystem.mkdirs(new Path("/aa/bb/cc"));
        //3.关闭FileSystem
        fileSystem.close();
    }
    //上传文件
    public void uploadFile() throws URISyntaxException, IOException, InterruptedException {
        //1、获取FileSystem 实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop0:8020"), new Configuration(), "root");

        //2、上传文件
        fileSystem.copyFromLocalFile(new Path("D://自然人标签.txt"),new Path("/aa/bb/cc"));

        //3、关闭FileSystem
        fileSystem.close();
    }
    }

