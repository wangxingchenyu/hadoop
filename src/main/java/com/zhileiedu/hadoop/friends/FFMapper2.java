package com.zhileiedu.hadoop.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 13:39
 */
public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text k = new Text();
	private Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// d  a,b,c,d有这些人关注了d

		String[] fields = value.toString().split("\t");
		String[] men = fields[1].split(",");
		v.set(fields[0]);
		for (int i = 0; i < men.length; i++) {
			for (int s = i + 1; s < men.length; s++) {
				if (men[i].compareTo(men[s]) > 0) {
					k.set(men[s] + "-" + men[i]);
				} else {
					k.set(men[i] + "-" + men[s]);
				}
				context.write(k,v);
			}
		}
	}
}
