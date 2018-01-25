package org.dan;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.dan.service.NameNodeProtocol;

public class RPCClient {

	private final String SERVER_ADDRESS = "127.0.0.1";
	private final int SERVER_PORT = 8888;
	
	private NameNodeProtocol client; 
	
	public RPCClient() throws Exception {
		client = RPC.getProxy(NameNodeProtocol.class, 100L, 
				new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT), new Configuration());
	}
	
	public String getMetaData(String path) {
		return client.getMetaData(path);
	}
	
	public static void main(String[] args) throws Exception {
		RPCClient client = new RPCClient();
		System.out.println(client.getMetaData("test"));
	}
}
