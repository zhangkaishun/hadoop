package org.apache.hadoop.examples.mapreduce.rpc;

public class HelloService implements IHello{

	@Override
	public String sayHello(String name) {
		return "hi "+name;
	}

}
