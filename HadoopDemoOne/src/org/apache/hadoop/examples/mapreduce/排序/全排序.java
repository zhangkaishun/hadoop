package org.apache.hadoop.examples.mapreduce.排序;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * 1) 对Math.min(10, splits.length)个split（输入分片）进行随机取样，对每个split取10000个样，总共10万个样 
2) 10万个样排序，根据reducer的数量(n)，取出间隔平均的n-1个样 
3) 将这个n-1个样写入partitionFile(_partition.lst，是一个SequenceFile)，key是取的样，值是nullValue 
4) 将partitionFile写入DistributedCache 
 * @author zhangkaishun
 *
 */
public class 全排序 {

	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
															//选中的概率  一个选取的样本数  最大读取inputSplit数量
		RandomSampler<Text, Text> sampler=new RandomSampler<Text,Text>(0.1, 1000, 10);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		//写入分区文件
		InputSampler.writePartitionFile(job, sampler);
		
	}
}
