package org.apache.hadoop.examples.test.单表关联;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SingleMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] strArray = new String[2];
		int i = 0;
		StringTokenizer token = new StringTokenizer(value.toString());
		while (token.hasMoreTokens()) {
			String nextToken = token.nextToken();
			if (nextToken.equals("child") || nextToken.equals("parent")) {
				continue;
			}
			strArray[i] = nextToken.toString();
			i++;
		}
		if (i != 0) {
			context.write(new Text(strArray[0]), new Text("$p," + strArray[1]));
			context.write(new Text(strArray[1]), new Text("$c," + strArray[0]));
		}
	}
}
