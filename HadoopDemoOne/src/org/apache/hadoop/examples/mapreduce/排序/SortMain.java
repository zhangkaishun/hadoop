package org.apache.hadoop.examples.mapreduce.ÅÅÐò;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class SortMain {

	public static void main(String[] args) throws Exception {
		Configuration conf =new Configuration();
		Job job =Job.getInstance(conf);
		job.setSortComparatorClass(SortComparator.class); //ÅÅÐò
		job.setGroupingComparatorClass(GroupingComparator.class); //·Ö×é
	}

}
