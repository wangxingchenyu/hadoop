package com.zhileiedu.hadoop.writeableCompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 18:31
 */
public class SortReduce extends Reducer<FlowBean, Text,Text,FlowBean> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value,key);
		}
	}
}
