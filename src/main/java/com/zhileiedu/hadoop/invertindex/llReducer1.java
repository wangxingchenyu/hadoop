package com.zhileiedu.hadoop.invertindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 11:20
 */
public class llReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable v = new IntWritable(1);

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(key, v);
	}
}
