package com.zhileiedu.hadoop.reduceJoin;

import com.zhileiedu.hadoop.reduceJoin.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 19:18
 */
public class RJDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(RJDriver.class);

		job.setMapperClass(RJMapper.class);
		job.setReducerClass(RJReduce.class);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		job.setGroupingComparatorClass(RJComparator.class);

		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\output"));

		boolean b = job.waitForCompletion(true);

		System.exit(b ? 0 : 1);

	}
}
