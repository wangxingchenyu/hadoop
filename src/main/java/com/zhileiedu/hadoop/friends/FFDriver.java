package com.zhileiedu.hadoop.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/26 13:39
 */
public class FFDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job1 = Job.getInstance(new Configuration());
		job1.setJarByClass(FFDriver.class);

		job1.setMapperClass(FFMapper1.class);
		job1.setReducerClass(FFReducer1.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job1, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job1, new Path("d:\\output"));

		boolean b = job1.waitForCompletion(true);

		if (b) {
			Job job2 = Job.getInstance(new Configuration());
			job2.setJarByClass(FFDriver.class);

			job2.setMapperClass(FFMapper2.class);
			job2.setReducerClass(FFReducer2.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job2, new Path("d:\\output"));
			FileOutputFormat.setOutputPath(job2, new Path("d:\\output3"));

			boolean b2 = job2.waitForCompletion(true);
			System.exit(b2 ? 0 : 1);
		}

	}
}
