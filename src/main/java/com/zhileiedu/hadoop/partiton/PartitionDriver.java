package com.zhileiedu.hadoop.partiton;

import com.zhileiedu.hadoop.flow.FlowBean;
import com.zhileiedu.hadoop.flow.FlowMapper;
import com.zhileiedu.hadoop.flow.FlowReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 17:09
 */
public class PartitionDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1 实例化job
		Job job = Job.getInstance(new Configuration());

		//2 设置类路径
		job.setJarByClass(PartitionDriver.class);

		//3 设置map reducer 类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReduce.class);

		//4 设置输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		//5 设置reduce输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 设置ReduceTask的数量
		job.setNumReduceTasks(5);
		job.setPartitionerClass(MyPartition.class);
		//6 设置数据输入输出路径
		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		FileOutputFormat.setOutputPath(job, new Path("d:\\output"));

		//7 提交
		boolean b = job.waitForCompletion(true);

		System.exit(b ? 0 : 1);

	}
}
