package com.zhileiedu.hadoop.topN;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: wzl
 * @Date: 2020/2/26 10:34
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		for (int i = 0; i < 10; i++) { // 取前10的数据
			if (iterator.hasNext()) {
				context.write(iterator.next(), key);
			}
		}
	}
}
