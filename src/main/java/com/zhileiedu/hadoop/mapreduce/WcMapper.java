package com.zhileiedu.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 11:45
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text text = new Text();
	private IntWritable intWritable = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("key=" + key.toString());
		String s = value.toString();
		String[] s1 = s.split(" ");
		for (String s2 : s1) {
			text.set(s2);
			context.write(text, intWritable);
		}
	}
}
