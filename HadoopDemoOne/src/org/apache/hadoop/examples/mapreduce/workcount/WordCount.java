package org.apache.hadoop.examples.mapreduce.workcount;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {

	public static void main(String[] args) {
		try {
			Configuration config=new Configuration();
			
			Job job=Job.getInstance(config);
			job.setUser("zhangkaishun");
			job.setJobName("wordcount ื๗าต");
			job.setJarByClass(WordCount.class);
			job.setPartitionerClass(MyPartition.class);
			job.setMapperClass(WCMap.class);
			job.setReducerClass(WCReduce.class);
			job.setCombinerClass(WCReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.setInputPaths(job, new Path("hdfs://namenode:9000/input/a.txt"));
			FileOutputFormat.setOutputPath(job, new Path("hdfs://namenode:9000/output/wordcount"));
			job.waitForCompletion(true);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
