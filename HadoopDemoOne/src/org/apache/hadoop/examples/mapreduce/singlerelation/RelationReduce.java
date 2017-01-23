package org.apache.hadoop.examples.mapreduce.singlerelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelationReduce extends Reducer<Text, Text, Text, Text>{

	private static int time=0;
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
		if(time==0){
			context.write(new Text("grandChildren"), new Text("grandParent"));
			time++;
		}
		List<String> grandChildrens=new ArrayList<String>();
		List<String> groundParents=new ArrayList<String>();
		for(Text text:value){
			String[] result = text.toString().split(":");
			if(result[0].endsWith("1")){
				groundParents.add(result[2]);
			}else{
				grandChildrens.add(result[1]);
			}
		}
		for(int k=0;k<grandChildrens.size();k++){
			for(int f=0;f<groundParents.size();f++){
				context.write(new Text(grandChildrens.get(k)), new Text(groundParents.get(f)));

			}
			
		}

	}
}
