package com.xyg.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AccessApp
{
    
     public static void main(String[] args) throws Exception {
//         System.setProperty("HADOOP_USER_NAME", "hadoop");
         System.setProperty("hadoop.home.dir", "G:\\hadoop\\hadoop-2.6.1");
         System.load("G:\\hadoop\\hadoop-2.6.1\\bin\\hadoop.dll");
         
         Configuration configuration = new Configuration(); 
//         configuration.set("fs.defaultFS", "hdfs://hadoop000:8020");
         
         Job job = Job.getInstance(configuration);
//         Job job = Job.getInstance();
         
         job.setMapperClass(AccessMapper.class);
         job.setReducerClass(AccessReducer.class);
         job.setPartitionerClass(AccessPartitioner.class);
         
         job.setNumReduceTasks(3);
         
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Access.class);
         
         job.setOutputKeyClass(NullWritable.class);
         job.setOutputValueClass(Access.class);
         
         Path inputPath = new Path("access/input/access.log"); 
         Path outputPath = new Path("access/output");
         FileInputFormat.setInputPaths(job, inputPath);
         FileOutputFormat.setOutputPath(job, outputPath);
         
         FileSystem fileSystem = FileSystem.get(configuration);
         if (fileSystem.exists(outputPath)) {
             fileSystem.delete(outputPath, true);
         }
         
         job.waitForCompletion(true);
    }
    
}
