package org.apache.hadoop.examples.mapreduce.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;


public class RPCServer {

	public static void main(String[] args) {

		Server server;
		try {
			server = new RPC.Builder(new Configuration())
			.setProtocol(IHello.class)
			.setInstance(new HelloService())
			.setBindAddress("172.18.1.85")
			.setPort(10001)
			.build();
			server.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
