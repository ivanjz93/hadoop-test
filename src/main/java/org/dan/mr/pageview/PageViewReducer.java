package org.dan.mr.pageview;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.dan.mr.accesslog.AccessBean;

public class PageViewReducer extends Reducer<Text, AccessBean, NullWritable, Text> {

	private Text outValue = new Text();
	private DateFormat myDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Override
	protected void reduce(Text key, Iterable<AccessBean> values, Context context) throws IOException, InterruptedException {
		List<AccessBean> accessBeans = new ArrayList<>();
		for(AccessBean value : values) {
			AccessBean accessBean = new AccessBean();
			try {
				BeanUtils.copyProperties(accessBean, value);
			} catch (IllegalAccessException | InvocationTargetException e) {
				e.printStackTrace();
				continue;
			}
			accessBeans.add(accessBean);
		}
		
		Collections.sort(accessBeans, (o1, o2) -> {
			AccessBean bean1 = (AccessBean)o1;
			AccessBean bean2 = (AccessBean)o2;
			Date date1 = getDate(bean1.getTime());
			Date date2 = getDate(bean2.getTime());
			if(date1 == null || date2 == null)
				return 0;
			return date1.compareTo(date2);
		});
		
		int step = 1;
		String session = UUID.randomUUID().toString();
		for(int i = 0; i < accessBeans.size(); i++) {
			AccessBean accessBean = accessBeans.get(i);
			if(accessBeans.size() == 1) {
				String line = session + "\u0001" + accessBean.getTime() + "\u0001" + accessBean.getUrl() 
					+ "\u0001" + "60" + "\u0001" + step;
				outValue.set(line);
				context.write(NullWritable.get(), outValue);
			}
			if(i == 0)
				continue;
			long timeDiff = diffTime(getDate(accessBean.getTime()), getDate(accessBeans.get(i - 1).getTime()));
			if(timeDiff < 30 * 60 * 1000) {
				AccessBean beforeBean = accessBeans.get(i - 1);
				String line = session + "\u0001" + beforeBean.getTime() + "\u0001" + beforeBean.getUrl() 
				+ "\u0001" + timeDiff  + "\u0001" + step;
				outValue.set(line);
				context.write(NullWritable.get(), outValue);
				step++;
			} else {
				AccessBean beforeBean = accessBeans.get(i - 1);
				String line = session + "\u0001" + beforeBean.getTime() + "\u0001" + beforeBean.getUrl() 
				+ "\u0001" + timeDiff  + "\u0001" + step;
				outValue.set(line);
				context.write(NullWritable.get(), outValue);
				step = 1;
				session = UUID.randomUUID().toString();
			}
			if(i == accessBeans.size() - 1) {
				String line = session + "\u0001" + accessBean.getTime() + "\u0001" + accessBean.getUrl() 
				+ "\u0001" + "60" + "\u0001" + step;
				outValue.set(line);
				context.write(NullWritable.get(), outValue);
			}
			
		}
	}
	
	private Date getDate(String dateStr) {
		try {
			return myDF.parse(dateStr);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private long diffTime(Date date1, Date date2) {
		return date1.getTime() - date2.getTime();
	}
}
