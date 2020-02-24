package com.zhileiedu.hadoop.groupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: wzl
 * @Date: 2020/2/23 22:11
 */
public class OrderComparator extends WritableComparator {

	protected OrderComparator() {
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean orderA = (OrderBean) a;
		OrderBean orderB = (OrderBean) b;

		// 只需要比较订单ID,就可以分在一个组里面
		return orderA.getOrderId().compareTo(orderB.getOrderId());

	}
}
