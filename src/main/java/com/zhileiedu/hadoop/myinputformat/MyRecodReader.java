package com.zhileiedu.hadoop.myinputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/23 15:01
 */
public class MyRecodReader extends RecordReader<Text, BytesWritable> {
	private boolean notRead = true;
	private Text key = new Text();
	private BytesWritable value = new BytesWritable();
	private FSDataInputStream inputStream;
	private FileSplit fs;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 转换切片类型到文件类型
		fs = (FileSplit) split;
		// 通过切片获取路径
		Path path = fs.getPath();

		// 通过路径获取文件系统
		FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

		context.getConfiguration();

		// 开流(打开这个文件)
		inputStream = fileSystem.open(path);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (notRead) {
			// 读key
			key.set(fs.getPath().toString());
			// 读value
			byte[] buf = new byte[(int) fs.getLength()];
			inputStream.read(buf); // 读取值并给buf参数,此时buf里面就有数据
			value.set(buf, 0, buf.length);
			notRead = false;
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return notRead ? 0 : 1;
	}

	@Override
	public void close() throws IOException {
		// 关流
		IOUtils.closeStream(inputStream);
	}
}
