package org.apache.hadoop.examples.mapreduce.排序.二次排序;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class 二次排序Main {
	
	public static void main(String[] args) {
		try {
		Configuration conf=new Configuration();
		//System.setProperty("hadoop.home.dir", "D:\\sortware\\hadoop-2.2.0");
		Job job=Job.getInstance(conf);
		job.setUser("zks");
		job.setNumReduceTasks(1);
		job.setJobName("二次排序");
		job.setMapperClass(二次排序Map.class);
		job.setReducerClass(二次排序Reduce.class);
		job.setJarByClass(二次排序Main.class);
		job.setGroupingComparatorClass(二次排序GroupSort.class);
		job.setPartitionerClass(二次排序Partition.class);
		//设置传递给reduce之前是如何排序
		job.setSortComparatorClass(二次排序ComparatorSort.class);
		job.setMapOutputKeyClass(CombinationKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job,new Path( "hdfs://namenode:9000/tmp/tmp/secondsort"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/tmp/output/secondsort"));
	
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
}
