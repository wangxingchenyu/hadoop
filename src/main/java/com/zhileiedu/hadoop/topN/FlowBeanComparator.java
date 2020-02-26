package com.zhileiedu.hadoop.topN;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: wzl
 * @Date: 2020/2/26 10:49
 */
public class FlowBeanComparator extends WritableComparator {

	protected FlowBeanComparator() {
		super(FlowBean.class, true);
	}


	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// 让所有的数据都在同一个组内
		return 0;
	}
}
