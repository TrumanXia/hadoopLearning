package com.xyg.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
写MR Job的时候遇到一个坑爹的异常：

LongWritable cannot be cast to org.apache.hadoop.io.IntWritable

当写Map的时候，key的默认输入就是LongWritable。

因为LongWritable指代Block中的数据偏移量。
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access>
{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Access>.Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
//        倒数第2个，可以用length - 2
        context.write(new Text(values[1]) , new Access(values[1], Integer.parseInt(values[8]), Integer.parseInt(values[9])));
    }
}
