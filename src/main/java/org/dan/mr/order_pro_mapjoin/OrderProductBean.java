package org.dan.mr.order_pro_mapjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderProductBean implements Writable{

	private String id;
	private String pid;
	private String dateStr;
	private String name;
	private String catagory_id;
	private double price;
	private int amount;
	private int flag; //0表示order文件一行构成的实体，1表示product文件一行构成的实体
	
	@Override
	public void readFields(DataInput input) throws IOException {
		id = input.readUTF();
		pid = input.readUTF();
		dateStr = input.readUTF();
		name = input.readUTF();
		catagory_id = input.readUTF();
		price = input.readDouble();
		amount = input.readInt();
		flag = input.readInt();
	}
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(id);
		output.writeUTF(pid);
		output.writeUTF(dateStr);
		output.writeUTF(name);
		output.writeUTF(catagory_id);
		output.writeDouble(price);
		output.writeInt(amount);
		output.writeInt(flag);
	}
	
	public void set(String id, String pid, String dateStr, String name, String catagory_id, double price, int amount,
			int flag) {
		this.id = id;
		this.pid = pid;
		this.dateStr = dateStr;
		this.name = name;
		this.catagory_id = catagory_id;
		this.price = price;
		this.amount = amount;
		this.flag = flag;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public String getDateStr() {
		return dateStr;
	}
	public void setDateStr(String dateStr) {
		this.dateStr = dateStr;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCatagory_id() {
		return catagory_id;
	}
	public void setCatagory_id(String catagory_id) {
		this.catagory_id = catagory_id;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}
	public int getAmount() {
		return amount;
	}
	public void setAmount(int amount) {
		this.amount = amount;
	}
	public int getFlag() {
		return flag;
	}
	public void setFlag(int flag) {
		this.flag = flag;
	}
	@Override
	public String toString() {
		return "id=" + id + ", pid=" + pid + ", dateStr=" + dateStr + ", name=" + name
				+ ", catagory_id=" + catagory_id + ", price=" + price + ", amount=" + amount;
	}
	
}
