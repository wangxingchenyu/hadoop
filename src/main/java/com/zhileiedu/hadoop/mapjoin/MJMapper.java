package com.zhileiedu.hadoop.mapjoin;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * @Author: wzl
 * @Date: 2020/2/25 11:20
 */
public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Map<String, String> pMap = new HashedMap();
	private Text k = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//super.setup(context);
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

		String line;
		while (StringUtils.isNotEmpty((line = reader.readLine()))) {
			String[] split = line.split("\t");
			pMap.put(split[0], split[1]);
		}

		reader.close();

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] split = line.split("\t");
		String pId = split[1];
		String pName = pMap.get(pId);
		k.set(line + "\t" + pName);

		context.write(k, NullWritable.get());

	}
}
