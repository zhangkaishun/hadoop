package org.apache.hadoop.examples.test.多表关联;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMap extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer token = new StringTokenizer(value.toString());
		String vKey = null;
		String vValue = null;
		int i = 0;
		while (token.hasMoreTokens()) {
			String nextToken = token.nextToken();
			if (nextToken.equals("factoryname")
					|| nextToken.equals("addressed")
					|| nextToken.equals("addressID")
					|| nextToken.equals("addressname")) {

				break;
			}
			if (nextToken.matches("^[0-9]*$")) {
				vKey = nextToken;
			} else {
				if (i == 0) {
					vValue = "F:" + nextToken;
				} else {
					vValue = "A:" + nextToken;
				}
			}
			i++;
		}
		if(vKey!=null&&vValue!=null){
		context.write(new Text(vKey), new Text(vValue));
		}
	}

}
