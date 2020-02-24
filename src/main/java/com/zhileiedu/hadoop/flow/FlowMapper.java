package com.zhileiedu.hadoop.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 17:09
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	private Text phone = new Text();
	private FlowBean flow = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		// 获取并设置电话
		phone.set(fields[1]);
		long upFlow = Long.parseLong(fields[fields.length - 3]);
		long downFlow = Long.parseLong(fields[fields.length - 2]);
		flow.set(upFlow, downFlow);
		context.write(phone, flow);
	}
}
