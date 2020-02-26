package com.zhileiedu.hadoop.ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/25 12:02
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(" ");
		if (split.length > 11) {
			context.getCounter("ETL", "true").increment(1);
			context.write(value, NullWritable.get());
		} else {
			context.getCounter("ETL", "false").increment(1);
		}

	}
}
