package org.apache.hadoop.examples.test.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> itear,
			Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable i:itear){
			sum=sum+i.get();
		}
		context.write(key, new IntWritable(sum));
		
	}
}
