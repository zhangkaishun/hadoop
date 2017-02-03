package org.apache.hadoop.examples.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class StudentUDF extends UDF{

	public Text evaluate(IntWritable in){
		return new Text("±àºÅ"+in);
	}
}
