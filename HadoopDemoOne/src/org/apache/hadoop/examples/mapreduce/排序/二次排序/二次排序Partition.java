package org.apache.hadoop.examples.mapreduce.排序.二次排序;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class 二次排序Partition extends Partitioner<CombinationKey, IntWritable> {

	@Override
	public int getPartition(CombinationKey combinationKey, IntWritable value, int numberPartitions) {
		// 按照firstKey 进行分区
		return combinationKey.getFirstKey().hashCode()&Integer.MAX_VALUE%numberPartitions;
	}

}
