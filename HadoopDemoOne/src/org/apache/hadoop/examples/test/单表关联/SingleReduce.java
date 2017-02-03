package org.apache.hadoop.examples.test.单表关联;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SingleReduce extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> iter, Context context)
			throws IOException, InterruptedException {

		List<String> groundParent = new ArrayList<String>();
		List<String> childChild = new ArrayList<String>();
		for (Text te : iter) {
			String[] split = te.toString().split(",");
			if (split[0].equals("$p")) {
				groundParent.add(split[1]);
			} else {
				childChild.add(split[1]);
			}
		}
		for (String groundP : groundParent) {
			for (String childC : childChild) {
				context.write(new Text(groundP), new Text(childC));
			}
		}
	}

}
