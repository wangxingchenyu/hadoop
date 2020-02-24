package com.zhileiedu.hadoop.groupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: wzl
 * @Date: 2020/2/23 22:11
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean,NullWritable> {
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		/*
		for (NullWritable value : values) { // 循环写入
			context.write(key,value);
		}*/
		// context.write(key,NullWritable.get());
		Iterator<NullWritable> iterator = values.iterator();
		for (int i = 0; i <2 ; i++) { // 输出价格的前两名
			if (iterator.hasNext()){
				NullWritable next = iterator.next();
				context.write(key,next);
			}
		}
	}
}
