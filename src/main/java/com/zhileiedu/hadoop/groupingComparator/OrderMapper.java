package com.zhileiedu.hadoop.groupingComparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 22:11
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

	private OrderBean order = new OrderBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit) context.getInputSplit();

		String[] fields = value.toString().split("\t");
		order.setOrderId(fields[0]);
		order.setProductId(fields[1]);
		order.setPrice(Double.parseDouble(fields[2]));

		context.write(order,NullWritable.get());
	}
}
