package org.dan.mr.max_order_price;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {

	public OrderIdGroupingComparator() {
		super(OrderBean.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean)a;
		OrderBean bBean = (OrderBean)b;
		return aBean.getOrderId().compareTo(bBean.getOrderId());
	}

}
