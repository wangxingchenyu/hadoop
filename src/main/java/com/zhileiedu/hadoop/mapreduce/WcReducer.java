package com.zhileiedu.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 11:45
 */
public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable v = new IntWritable();

	/**
	 * reduce的操作相当于已经给相同的key汇总在一起了，已经判断了key是否相同的判断
	 *
	 * @param key
	 * @param values
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		// super.reduce(key, values, context);
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		v.set(sum);
		context.write(key, v);
	}
}
