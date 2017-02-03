package org.apache.hadoop.examples.test.单表关联;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SingleMain {

	public static void main(String[] args) throws Exception {
		Configuration config=new Configuration();
		Job job=Job.getInstance(config);
		job.setJobName("字符统计任务");
		job.setMapperClass(SingleMap.class);
		job.setReducerClass(SingleReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/input/people"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/output/people"));
		job.waitForCompletion(true);
	}

}
