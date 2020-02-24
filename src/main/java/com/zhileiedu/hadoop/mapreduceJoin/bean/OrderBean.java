package com.zhileiedu.hadoop.mapreduceJoin.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/2/24 19:17
 */
public class OrderBean implements WritableComparable<OrderBean> {

	private String id;
	private String pid;
	private int amout;
	private String pname;

	@Override
	public String toString() {
		return id + "\t" + pname + "\t" + amout;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmout() {
		return amout;
	}

	public void setAmout(int amout) {
		this.amout = amout;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	@Override
	public int compareTo(OrderBean o) { // 如果pid相同，则比较pname
		int compare = this.pid.compareTo(o.pid);
		if (compare == 0) {
			return o.pname.compareTo(this.pname);
		} else {
			return compare;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.id);
		out.writeUTF(this.pid);
		out.writeInt(this.amout);
		out.writeUTF(this.pname);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readUTF();
		this.pid = in.readUTF();
		this.amout = in.readInt();
		this.pname = in.readUTF();
	}
}
