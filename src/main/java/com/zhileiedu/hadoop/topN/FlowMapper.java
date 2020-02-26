package com.zhileiedu.hadoop.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 10:34
 */
public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	private FlowBean flowBean = new FlowBean();
	private Text phone = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		phone.set(fields[0]);
		flowBean.set(Long.parseLong(fields[1]), Long.parseLong(fields[2]));
		context.write(flowBean, phone);
	}
}
