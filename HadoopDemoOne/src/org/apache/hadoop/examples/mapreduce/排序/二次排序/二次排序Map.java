package org.apache.hadoop.examples.mapreduce.排序.二次排序;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class 二次排序Map extends Mapper<LongWritable, Text, CombinationKey, IntWritable>{

	CombinationKey combinationKey=new CombinationKey();
	String[] split=null;
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		String key1=split[0];
		String value1=split[1];
		combinationKey.setFirstKey(new Text(key1+","+value1));
		combinationKey.setSecondKey(new IntWritable(Integer.parseInt(value1)));
		context.write(combinationKey, new IntWritable(Integer.parseInt(value1)));
	}

	
}
