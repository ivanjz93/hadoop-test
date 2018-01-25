package org.dan.mr.order_pro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, OrderProductBean, OrderProductBean, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<OrderProductBean> values, Context context)
			throws IOException, InterruptedException {
		OrderProductBean productBean = new OrderProductBean();
		List<OrderProductBean> orderBeans = new ArrayList<>();
		for(OrderProductBean bean : values) {
			if(bean.getFlag() == 1) {
				try {
					BeanUtils.copyProperties(productBean, bean);
				} catch(Exception e) {
					e.printStackTrace();
				}
			} else {
				try {
					OrderProductBean orderBean = new OrderProductBean();
					BeanUtils.copyProperties(orderBean, bean);
					orderBeans.add(orderBean);
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
		}
		for(OrderProductBean orderBean : orderBeans) {
			orderBean.setPid(productBean.getPid());
			orderBean.setName(productBean.getName());
			orderBean.setCatagory_id(productBean.getCatagory_id());
			orderBean.setPrice(productBean.getPrice());
			context.write(orderBean, NullWritable.get());
		}
	}

}
