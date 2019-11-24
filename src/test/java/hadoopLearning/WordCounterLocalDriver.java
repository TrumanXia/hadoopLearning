package hadoopLearning;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.spi.scanning.FilesScanner;

public class WordCounterLocalDriver
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        System.setProperty("hadoop.home.dir", "G:\\hadoop\\hadoop-2.6.1");
//        System.load("G:\\hadoop\\hadoop-2.6.1\\bin\\hadoop.dll");
        
        Configuration configuration = new Configuration(); 
//        configuration.set("fs.defaultFS", "hdfs://hadoop000:8020");
        
        Job job = Job.getInstance(configuration);
        
        job.setJarByClass(WordCounterLocalDriver.class);
        
        job.setMapperClass(WordCounterMapper.class);
        job.setReducerClass(WordCounterReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        设置为本地路径就行
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output/"));
        
//        如果文件存在，删除文件
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path("/wordcount/output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(new Path("/wordcount/output"), true);
        }
        
        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
