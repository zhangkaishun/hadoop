package org.apache.hadoop.examples.mapreduce.singlerelation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class People implements Writable{
	private String child;
	private String parent;
	private String groundParent;
	public String getChild() {
		return child;
	}
	public void setChild(String child) {
		this.child = child;
	}
	public String getParent() {
		return parent;
	}
	public void setParent(String parent) {
		this.parent = parent;
	}
	public String getGroundParent() {
		return groundParent;
	}
	public void setGroundParent(String groundParent) {
		this.groundParent = groundParent;
	}
	@Override
	public String toString() {
		return "People [child=" + child + ", parent=" + parent
				+ ", groundParent=" + groundParent + "]";
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.child=arg0.readUTF();
		this.parent=arg0.readUTF();
		this.groundParent=arg0.readUTF();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeUTF(this.child);
		arg0.writeUTF(this.parent);
		arg0.writeUTF(this.groundParent);
	}
	
}
