package com.zhileiedu.hadoop.reduceJoin;

import com.zhileiedu.hadoop.reduceJoin.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: wzl
 * @Date: 2020/2/24 19:52
 */
public class RJComparator extends WritableComparator {

	protected RJComparator() {// 写一个构造器来
		super(OrderBean.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean oa = (OrderBean) a;
		OrderBean ob = (OrderBean) b;

		return oa.getPid().compareTo(ob.getPid());


	}
}
