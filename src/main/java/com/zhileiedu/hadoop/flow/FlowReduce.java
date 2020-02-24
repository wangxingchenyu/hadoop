package com.zhileiedu.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 17:09
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
	private FlowBean sumFlow = new FlowBean();

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
		long sumUpFlow = 0;
		long sumDownFlow = 0;

		for (FlowBean value : values) {
			sumUpFlow += value.getUpFlow();
			sumDownFlow += value.getDownFlow();
		}

		sumFlow.set(sumUpFlow,sumDownFlow);
		context.write(key, sumFlow);

	}
}
