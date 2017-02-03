package org.apache.hadoop.examples.mapreduce.≈≈–Ú.∂˛¥Œ≈≈–Ú;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CombinationKey implements Writable{

	private Text firstKey;
	private IntWritable secondKey;
	
	
	public Text getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(Text firstKey) {
		this.firstKey = firstKey;
	}

	public IntWritable getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(IntWritable secondKey) {
		this.secondKey = secondKey;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.firstKey=new Text(in.readUTF());
		this.secondKey=new IntWritable(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.firstKey.toString());
		out.writeInt(this.secondKey.get());
	}

	




	
}
