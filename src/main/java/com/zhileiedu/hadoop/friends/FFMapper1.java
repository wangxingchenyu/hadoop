package com.zhileiedu.hadoop.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 13:39
 */
public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// A:B,C,E 算出 B,C,E是谁的好友
		String[] fields = value.toString().split(":");
		v.set(fields[0]);
		if (fields.length > 1) {
			for (String men : fields[1].split(",")) {
				k.set(men);
				context.write(k, v);
			}
		}
	}
}
