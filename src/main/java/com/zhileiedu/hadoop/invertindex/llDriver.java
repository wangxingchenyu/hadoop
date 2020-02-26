package com.zhileiedu.hadoop.invertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 11:20
 * <p>
 * 倒排索引,搜索引擎
 */
public class llDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job1 = Job.getInstance(new Configuration());

		job1.setJarByClass(llDriver.class);
		job1.setMapperClass(llMapper1.class);
		job1.setReducerClass(llReducer1.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job1, new Path("d:\\output"));

		boolean b = job1.waitForCompletion(true);

		if (b) { // 第一个job执行完成了，执行第二个job
			Job job2 = Job.getInstance(new Configuration());

			job2.setJarByClass(llDriver.class);
			job2.setMapperClass(llMapper2.class);
			job2.setReducerClass(llReducer2.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job2, new Path("d:\\output"));
			FileOutputFormat.setOutputPath(job2, new Path("d:\\output2"));

			boolean b2 = job2.waitForCompletion(true);

			System.exit(b2 ? 0 : 1);

		}

	}
}
