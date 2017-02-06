package org.apache.hadoop.examples.mapreduce.排序.二次排序;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class 二次排序ComparatorSort extends WritableComparator {

	/**
	 * 切记重写无参构造方法
	 */
	public 二次排序ComparatorSort() {
		super(CombinationKey.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CombinationKey combinationKeyOne = (CombinationKey) a;
		CombinationKey combinationKeyTwo = (CombinationKey) b;
		// 如果firstkey 不相同就按照 firstKey排序
		if (!combinationKeyOne.getFirstKey().equals(
				combinationKeyTwo.getFirstKey())) {
			return combinationKeyOne.getFirstKey().compareTo(
					combinationKeyTwo.getFirstKey());
		} else {
			// firstKey 相同再按照secondKey排序
			return combinationKeyOne.getSecondKey().get()
					- combinationKeyTwo.getSecondKey().get();
		}
	}
}
