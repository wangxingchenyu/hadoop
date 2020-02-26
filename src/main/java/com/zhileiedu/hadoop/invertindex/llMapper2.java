package com.zhileiedu.hadoop.invertindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 11:20
 */
public class llMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 上次reduce处理完的数据是 atguigu--a.txt  3
		String[] split = value.toString().split("--");
		k.set(split[0]);
		String[] fields = split[1].split("\t");
		// 封装成atguigu a.txt-->3
		v.set(fields[0] + "-->" + fields[1]);
		context.write(k, v);
	}
}
