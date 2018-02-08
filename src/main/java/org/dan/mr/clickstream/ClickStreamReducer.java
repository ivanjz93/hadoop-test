package org.dan.mr.clickstream;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ClickStreamReducer extends Reducer<Text, PageView, NullWritable, VisitBean> {

	private VisitBean outValue = new VisitBean();
	
	@Override
	protected void reduce(Text key, Iterable<PageView> values, Context context) throws IOException, InterruptedException {
		List<PageView> pageViews = new ArrayList<>();
		for(PageView value : values) {
			PageView pageView = new PageView();
			try {
				BeanUtils.copyProperties(pageView, value);
			} catch (IllegalAccessException | InvocationTargetException e) {
				e.printStackTrace();
				continue;
			}
			pageViews.add(pageView);
		}
		if(pageViews.size() == 1) {
			PageView oneView = pageViews.get(0);
			outValue.set(key.toString(), oneView.getTime(), oneView.getTime(), oneView.getUrl(), oneView.getUrl(), pageViews.size());
		} else {
			Collections.sort(pageViews, (o1, o2) -> {
				PageView p1 = (PageView)o1;
				PageView p2 = (PageView)o2;
				return Integer.parseInt(p1.getStep()) - Integer.parseInt(p2.getStep());
			});
			
			PageView firstPage = pageViews.get(0);
			PageView endPage = pageViews.get(pageViews.size() - 1);
			outValue.set(key.toString(), firstPage.getTime(), endPage.getTime(), firstPage.getUrl(), endPage.getUrl(), pageViews.size());
		}
		context.write(NullWritable.get(), outValue);
	}

}
