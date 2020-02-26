package com.zhileiedu.hadoop.invertindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 11:20
 */
public class llReducer2 extends Reducer<Text, Text, Text, Text> {
	private Text value = new Text();
	private StringBuffer sb = new StringBuffer();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// 先清空下stringBuilder
		sb.delete(0, sb.length());
		for (Text value : values) {
			sb.append(value.toString()).append(" ");
		}
		// 封装成 aigugi one.txt->3  two.txt->4
		value.set(sb.toString());
		context.write(key, value);
	}
}
