package org.apache.hadoop.examples.mapreduce.manyrelation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ManyReduce extends Reducer<IntWritable, Text, Text,Text >{

	String addressName=new String();
	String factoryName=new String();
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
	

		for(Text valueText:values){
			String value=valueText.toString();
			if(value.startsWith("A")){
				addressName=value.split("-")[1];
			}else{
				factoryName=value.split("-")[1];
			}
		}
		context.write(new Text(factoryName), new Text(addressName));
	}

	
	

}
