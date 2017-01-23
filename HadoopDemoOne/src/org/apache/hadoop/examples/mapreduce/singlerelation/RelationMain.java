package org.apache.hadoop.examples.mapreduce.singlerelation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class RelationMain {

	public static void main(String[] args) throws Exception {
		Job job=Job.getInstance();
		job.setJobName("单表关联");
		job.setJarByClass(RelationMain.class); // 通过传入的class 找到job的jar包
		job.setMapperClass(RelationMap.class);
		job.setReducerClass(RelationReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/input/people"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/output/outpeople"));
		job.waitForCompletion(true);
	}

}
