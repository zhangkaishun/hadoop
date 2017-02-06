package org.apache.hadoop.examples.mapreduce.排序.二次排序;

import org.apache.hadoop.io.WritableComparator;


/**
 * 分组是指哪些key一样的，将value封装为一个集合
 * 如 1 2，1 3 封装为1 【2，3】
 * 
 * 
 * @author hasee
 *
 */
public class 二次排序GroupSort extends WritableComparator {

	public 二次排序GroupSort() {
		super(CombinationKey.class,true);
	}
	@Override
	public int compare(Object a, Object b) {
		CombinationKey keyOne = (CombinationKey) a;
		CombinationKey keyTwo = (CombinationKey) b;
		return keyOne.getFirstKey().compareTo(keyTwo.getFirstKey());
	}
}
