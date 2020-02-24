package com.zhileiedu.hadoop.flow;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/22 17:10
 */
public class FlowBean implements Writable {  // Writable 这个词没有 "e" ,别引错包
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	public FlowBean() {
	}

	/**
	 * @param upFlow   上行
	 * @param downFlow 下行
	 */
	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}


	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	/**
	 * Serialize the fields of this object to <code>out</code>.
	 *
	 * @param out <code>DataOuput</code> to serialize this object into.
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException { // 序列化
		out.writeLong(this.upFlow);
		out.writeLong(this.downFlow);
		out.writeLong(this.sumFlow);
	}

	/**
	 * Deserialize the fields of this object from <code>in</code>.
	 *
	 * <p>For efficiency, implementations should attempt to re-use storage in the
	 * existing object where possible.</p>
	 *
	 * @param in <code>DataInput</code> to deseriablize this object from.
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException { // 反序列化
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
}
