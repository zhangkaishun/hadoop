package org.apache.hadoop.examples.test.多表关联;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MMain {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJobName("多表关联");
		job.setJarByClass(MMain.class);
		job.setMapperClass(MMap.class);
		job.setReducerClass(MReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path[] { new Path(
				"hdfs://namenode:9000/input/factory.txt"), new Path(
						"hdfs://namenode:9000/input/address.txt") });
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://namenode:9000/output/address.txt"));
		job.waitForCompletion(true);
	}

}
