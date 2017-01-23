package org.apache.hadoop.examples.mapreduce.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class RPCClient {

	public static void main(String args[]) {
		try {
			IHello hello = RPC.getProxy(IHello.class, 1001,
					new InetSocketAddress("172.18.1.85", 10001),
					new Configuration());
			String sayHello = hello.sayHello("zhang san");
			System.out.println(sayHello);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
