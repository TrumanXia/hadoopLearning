package com.xyg.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessReducer extends Reducer<Text, Access, NullWritable, Access>
{
    @Override
    protected void reduce(Text key, Iterable<Access> values, Reducer<Text, Access, NullWritable, Access>.Context context)
            throws IOException, InterruptedException {
        int upStreamSum = 0;
        int downStreamSum = 0;
        Iterator<Access> iterator = values.iterator();
        while(iterator.hasNext()) {
            Access value = iterator.next();
            downStreamSum += value.getDownStream();
            upStreamSum += value.getUpStream();
        }
        context.write(null, new Access(key.toString(), downStreamSum, upStreamSum));
    }
}
