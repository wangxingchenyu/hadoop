package com.zhileiedu.hadoop.combiner;

import com.zhileiedu.hadoop.mapreduce.WcMapper;
import com.zhileiedu.hadoop.mapreduce.WcReducer;
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
 * @Date: 2020/2/22 11:46
 */
public class CombinercDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 0 获取Job对象
		Job job = Job.getInstance(new Configuration());
		// 1 设置jar存储的位置
		job.setJarByClass(CombinercDriver.class);

		// 2 关联Map和Reduce类
		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReducer.class);

		// 3 设置 Mapper阶段输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置最终的输出的key,value的数据类型 (也就是reduce)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置combiner
		// job.setCombinerClass(WcReducer.class);

		// 设置输入的路径
		FileInputFormat.setInputPaths(job, new Path("d:\\input"));
		// 设置输出的路径
		FileOutputFormat.setOutputPath(job, new Path("d:\\output_combiner"));
		// 提交job任务
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
