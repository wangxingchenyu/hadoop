package com.zhileiedu.hadoop.invertindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 11:20
 */
public class llMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text k = new Text();
	private IntWritable v = new IntWritable(1);
	private String filename;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		// 获取输入的文件
		filename = fileSplit.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		for (String field : fields) {
			k.set(field + "--" + filename);
			context.write(k, v);
		}
	}
}
