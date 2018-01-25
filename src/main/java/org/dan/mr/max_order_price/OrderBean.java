package org.dan.mr.max_order_price;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{

	private String orderId;
	private String productId;
	private DoubleWritable price;
	
	
	public void set(String orderId, String productId, double price) {
		this.orderId = orderId;
		this.productId = productId;
		this.price = new DoubleWritable(price);
	}
	
	public String getOrderId() {
		return orderId;
	}
	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	public DoubleWritable getPrice() {
		return price;
	}
	public void setPrice(DoubleWritable price) {
		this.price = price;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		orderId = input.readUTF();
		productId = input.readUTF();
		price = new DoubleWritable(input.readDouble());
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(orderId);
		output.writeUTF(productId);
		output.writeDouble(price.get());
	}
	
	@Override
	public int compareTo(OrderBean other) {
		int result;
		result = this.orderId.compareTo(other.getOrderId());
		if(result == 0){
			result = -this.price.compareTo(other.getPrice());
		}
		return result;
	}

	@Override
	public String toString() {
		return "orderId=" + orderId + ", productId=" + productId + ", price=" + price;
	}
	
}
