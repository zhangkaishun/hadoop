package org.apache.hadoop.examples.mapreduce.≈≈–Ú;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ≈≈–Ú
 * @author zhangkaishun
 *
 */
public class SortComparator extends WritableComparator{

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		return a.compareTo(b);
	}
}
