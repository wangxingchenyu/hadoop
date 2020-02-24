package com.zhileiedu.hadoop.partiton;

import com.zhileiedu.hadoop.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author: wzl
 * @Date: 2020/2/23 17:56
 */
public class MyPartition extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
		String phone = text.toString();
		switch (phone.substring(0, 3)) { // 取号码前三位
			case "136":
				return 0; // 这个代表第1个task 数字写几则代表+1个task
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
