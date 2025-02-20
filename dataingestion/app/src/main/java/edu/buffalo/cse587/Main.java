package edu.buffalo.cse587;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Main {
    public static void main(String[] args) {
        // keep the config
        String uri = "hdfs://namenode:8020";
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");

        String filePath = "test.txt";

        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(uri), conf);

            Path writePath = new Path(filePath);
            FSDataOutputStream outputStream = fs.create(writePath);
            String data = "Hello, HDFS!";
            outputStream.writeUTF(data);
            outputStream.close();
            System.out.println("Write to：" + filePath);

            Path readPath = new Path(filePath);
            FSDataInputStream inputStream = fs.open(readPath);
            String content = inputStream.readUTF();
            inputStream.close();
            System.out.println("Read from HDFS:" + content);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
