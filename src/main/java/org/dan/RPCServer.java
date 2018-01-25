package org.dan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;
import org.dan.service.NameNodeProtocol;
import org.dan.service.NameNodeProtocolImp;

public class RPCServer {
	
	private final String SERVER_ADDRESS = "127.0.0.1";
	private final int SERVER_PORT = 8888;
	private Server server;
	
	public RPCServer() throws Exception{
		Builder builder = new RPC.Builder(new Configuration());
		builder.setBindAddress(SERVER_ADDRESS)
			.setPort(SERVER_PORT)
			.setProtocol(NameNodeProtocol.class)
			.setInstance(new NameNodeProtocolImp());
		server = builder.build();
	}
	
	public void startServer() {
		server.start();
	}

	public static void main(String[] args) throws Exception {
		RPCServer server = new RPCServer();
		server.startServer();
	}

}
