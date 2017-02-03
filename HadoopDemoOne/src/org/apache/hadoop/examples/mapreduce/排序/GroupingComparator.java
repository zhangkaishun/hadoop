package org.apache.hadoop.examples.mapreduce.排序;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组，在reduce执行前，将相同key的value放到一个集合中
 * @author zhangkaishun
 *
 */
public class GroupingComparator extends WritableComparator {

	/**
	 * 按照key的前四位进行分组
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return super.compare(a.toString().substring(0, 5), b.toString().substring(0, 5));
	}
}
