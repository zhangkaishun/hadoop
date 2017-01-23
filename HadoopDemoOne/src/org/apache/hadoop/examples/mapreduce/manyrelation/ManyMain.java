package org.apache.hadoop.examples.mapreduce.manyrelation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ManyMain {

	public static void main(String[] args) {

		try {
			Job job=Job.getInstance();
			job.setMapperClass(ManyMap.class);
			job.setReducerClass(ManyReduce.class);
			job.setJarByClass(ManyMain.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job,new Path[]{new Path("hdfs://namenode:9000/input/address.txt"),new Path("hdfs://namenode:9000/input/factory.txt")});
			FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/output/manyreduce"));
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
