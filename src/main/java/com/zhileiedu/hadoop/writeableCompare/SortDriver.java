package com.zhileiedu.hadoop.writeableCompare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 18:31
 */
public class SortDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Job job= Job.getInstance(new Configuration());
		job.setJarByClass(SortDriver.class);

		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReduce.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job,new Path("d:\\output")); // 已经输出的数据
		FileOutputFormat.setOutputPath(job,new Path("d:\\output2")); // 新输出的排序过的内容

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);

	}
}
