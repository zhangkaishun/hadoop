package org.apache.hadoop.examples.test.多表关联;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> iter, Context context)
			throws IOException, InterruptedException {
		String facotryName = null;
		String address = null;
		for (Text text : iter) {
			String[] split = text.toString().split(":");
			if (split[0].equals("F")) {
				facotryName = split[1];
			} else {
				address = split[1];
			}
		}
		if (facotryName != null && address != null) {
			context.write(new Text(facotryName), new Text(address));
		}
	}

}
