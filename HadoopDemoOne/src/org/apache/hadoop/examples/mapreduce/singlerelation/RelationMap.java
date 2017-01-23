package org.apache.hadoop.examples.mapreduce.singlerelation;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RelationMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer token = new StringTokenizer(value.toString());
		String[] split=new String[2];
		int i=0;
		while (token.hasMoreTokens()) {
			String str = token.nextToken();
			if (str.equalsIgnoreCase("child")) {
				return;
			}
			split[i]=str;
			i++;
			
		}
		context.write(new Text(split[0]), new Text("1" + ":" + split[0] + ":"
				+ split[1]));
		context.write(new Text(split[1]), new Text("2" + ":" + split[0] + ":"
				+ split[1]));

	}

}
