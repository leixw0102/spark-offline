package com.ehl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;


/**
 * Created by 雷晓武 on 2017/2/16.
 */
public class HdfsUtils {

    private static Configuration configuration = new Configuration();


    public static void sendDirectory(Path from,Path to,String user) {

        System.setProperty("HADOOP_USER_NAME", user);

        DistributedFileSystem fs =null;
        try {
            fs = (DistributedFileSystem)FileSystem.get(configuration);
            fs.copyFromLocalFile(false,true,from,to);
            //send tag file
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void sendDirectory(Path from,Path to) {
        sendDirectory(from,to,"root");
    }
}
