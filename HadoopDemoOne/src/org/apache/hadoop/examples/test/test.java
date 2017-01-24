package org.apache.hadoop.examples.test;

import org.junit.Test;

public class test {
	@Test
	public void test1(){
		String ss="1239999";
		System.out.print(ss.matches("^[0-9]*$"));
	}
}
