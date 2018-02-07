package org.dan.mr.accesslog;

//219.141.189.148 - - [07/Feb/2018:12:58:42 +0800] "GET /zhhw/external/v1/trashcan?sn=863703032592581 HTTP/1.1" 200 243
public class AccessBean {
	private boolean invalid;
	private String ip;
	private String time;
	private String method;
	private String url;
	private String code;
	private String bytes;
	
	public void set(boolean invalid, String ip, String time, String method, String url, String code, String bytes) {
		this.invalid = invalid;
		this.ip = ip;
		this.time = time;
		this.method = method;
		this.url = url;
		this.code = code;
		this.bytes = bytes;
	}
	
	public boolean isInvalid() {
		return invalid;
	}
	public void setInvalid(boolean invalid) {
		this.invalid = invalid;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getTime() {
		return time;
	}
	public void setTime(String time) {
		this.time = time;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getBytes() {
		return bytes;
	}
	public void setBytes(String bytes) {
		this.bytes = bytes;
	}

	@Override
	public String toString() {
		return invalid + "\u0001" + ip + "\u0001" + time + "\u0001" + method + "\u0001"
				+ url + "\u0001" + code + "\u0001" + bytes;
	}
	
}
