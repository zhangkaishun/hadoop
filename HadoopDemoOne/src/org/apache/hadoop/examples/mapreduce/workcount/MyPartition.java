package org.apache.hadoop.examples.mapreduce.workcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartition extends HashPartitioner<Text, Text>{

	/**
	 * key 为map输出的键 value为map输出的值
	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		return key.hashCode()%numReduceTasks;
		
	};
}
