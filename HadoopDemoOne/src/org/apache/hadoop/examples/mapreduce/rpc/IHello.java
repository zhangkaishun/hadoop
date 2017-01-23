package org.apache.hadoop.examples.mapreduce.rpc;

public interface IHello {
	public static int versionID=100;
	String sayHello(String name);

}
