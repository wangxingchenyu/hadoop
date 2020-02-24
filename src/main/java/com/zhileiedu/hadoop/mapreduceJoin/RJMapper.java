package com.zhileiedu.hadoop.mapreduceJoin;

import com.zhileiedu.hadoop.mapreduceJoin.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 19:17
 */
public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	private OrderBean orderBean  = new OrderBean();
	private String filename;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit fs = (FileSplit) context.getInputSplit();
		// 获取文件名
		filename = fs.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		if (filename.equals("order.txt")){
			orderBean.setId(fields[0]);
			orderBean.setPid(fields[1]);
			orderBean.setAmout(Integer.parseInt(fields[2]));
			orderBean.setPname("");
		} else{
			orderBean.setPid(fields[0]);
			orderBean.setPname(fields[1]);
			orderBean.setId("");
			orderBean.setAmout(0);
		}

		context.write(orderBean,NullWritable.get());
	}
}
