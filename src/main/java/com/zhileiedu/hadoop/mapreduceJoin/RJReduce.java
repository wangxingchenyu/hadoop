package com.zhileiedu.hadoop.mapreduceJoin;

import com.zhileiedu.hadoop.mapreduceJoin.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: wzl
 * @Date: 2020/2/24 19:18
 */
public class RJReduce extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		String name = key.getPname();
		Iterator<NullWritable> iterator = values.iterator();
		iterator.next();
		while (iterator.hasNext()) {
			iterator.next();
			key.setPname(name);
			context.write(key, NullWritable.get());
		}

	}
}
