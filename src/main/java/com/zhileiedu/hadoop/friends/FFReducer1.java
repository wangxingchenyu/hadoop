package com.zhileiedu.hadoop.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 13:39
 */
public class FFReducer1 extends Reducer<Text, Text, Text, Text> {

	private Text v = new Text();
	private StringBuffer sb = new StringBuffer();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		sb.delete(0, sb.length());
		for (Text value : values) {
			sb.append(value.toString()).append(","); // v
		}
		v.set(sb.toString());

		context.write(key, v);
	}
}
