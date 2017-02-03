package org.apache.hadoop.examples.test.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception{
		
		Configuration config=new Configuration();
		Job job=Job.getInstance(config);
		job.setJobName("字符统计任务");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WMap.class);
		job.setReducerClass(WReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/sqoop/tmp/student/part-m-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/output/wordoutput"));
		job.waitForCompletion(true);
		System.out.println("执行成功");
	}
}
