package com.zhileiedu.hadoop.writeableCompare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 18:31
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	private Text phone = new Text();
	private FlowBean flow = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String s = value.toString();
		String[] fields = s.split("\t");
		phone.set(fields[0]);
		flow.setUpFlow(Long.parseLong(fields[1]));
		flow.setDownFlow(Long.parseLong(fields[2]));
		flow.setSumFlow(Long.parseLong(fields[3]));
		context.write(flow, phone);
	}
}
