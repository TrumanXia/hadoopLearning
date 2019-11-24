package hadoopLearning;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// InputKey InputValue OutputKey OutputValue
/**
 * 
写MR Job的时候遇到一个坑爹的异常：

LongWritable cannot be cast to org.apache.hadoop.io.IntWritable

当写Map的时候，key的默认输入就是LongWritable。

因为LongWritable指代Block中的数据偏移量。
 */
public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
//    为什么知道是一行数据？约定俗成
    @Override
    public void map(LongWritable keyIn, Text ValueIn, Context context) throws IOException, InterruptedException {
        String[] values = ValueIn.toString().split("\t");

        for (String value : values) {
            context.write(new Text(value), new IntWritable(1));
        }
    }
}
