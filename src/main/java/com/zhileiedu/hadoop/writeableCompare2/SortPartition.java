package com.zhileiedu.hadoop.writeableCompare2;

import com.zhileiedu.hadoop.writeableCompare.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: wzl
 * @Date: 2020/2/23 19:16
 */
public class SortPartition extends Partitioner<FlowBean, Text> {

	@Override
	public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
		switch (text.toString().substring(0, 3)) { // 分区接的也是map后的结果
			case "136":
				return 0;
			case "137":
				return 1;
			case "138":
				return 2;
			case "139":
				return 3;
			default:
				return 4;
		}
	}
}
