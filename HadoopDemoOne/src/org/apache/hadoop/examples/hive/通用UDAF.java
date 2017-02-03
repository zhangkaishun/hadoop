package org.apache.hadoop.examples.hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;


/**
 * 自定义UDAF统计b字段大于30的记录个数 countbigthan(b,30)实现代码
 * @author zhangkaishun
 *
 */
public class 通用UDAF extends AbstractGenericUDAFResolver{

	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
			throws SemanticException {
		return super.getEvaluator(info);
	}

	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info)
			throws SemanticException {
		return super.getEvaluator(info);
	}

	public static class GenericUDAFCountBigThanEvalutor extends GenericUDAFEvaluator
	{
		private LongWritable result;
		
		private PrimitiveObjectInspector inputOI1;
		private PrimitiveObjectInspector inputOI2;
		
		/**
		 * init 方法map，reduce阶段都得执行
		 * map阶段 parameters长度与UDAF输入的参数个数有关
		 * reduce阶段 parameters长度为1
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {
			result=new LongWritable();
			inputOI1=(PrimitiveObjectInspector) parameters[0];
			if(parameters.length>1){
				inputOI2=(PrimitiveObjectInspector) parameters[1];
			}
			//最终结果返回类型
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			CountAgg agg=new CountAgg(); //存放部分聚合值
			reset(agg);
			return agg;
		}

		/**
		 * 具体的逻辑，由于业务需要，此处只在map端运算
		 */
		@Override
		public void iterate(AggregationBuffer aggregationbuffer, Object[] parameters)
				throws HiveException {
			if(parameters==null||parameters.length<2){
				return;
			}
			double base=PrimitiveObjectInspectorUtils.getDouble(parameters[0], inputOI1);
			double tmp=PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputOI2);
			if(base>tmp){
				((CountAgg)aggregationbuffer).count++;
			}
		}

		/**
		 * 合并部分结果 map阶段（含有combiner）和reduce都执行
		 * parial 传递terminatePartial 得到的部分结果
		 */
		@Override
		public void merge(AggregationBuffer aggregationbuffer, Object obj)
				throws HiveException {
			if(obj!=null){
				long p=PrimitiveObjectInspectorUtils.getLong(obj, inputOI1);
				((CountAgg)aggregationbuffer).count+=p;
			}
				
		}

		@Override
		public void reset(AggregationBuffer aggregationbuffer)
				throws HiveException {
			CountAgg countagg=(CountAgg) aggregationbuffer;
			countagg.count=0;
		}

		
		@Override
		public Object terminate(AggregationBuffer aggregationbuffer)
				throws HiveException {
			result.set(((CountAgg)aggregationbuffer).count);
			return result;
		}

			/**
			 * map 阶段返回部分结果
			 */
		@Override
		public Object terminatePartial(AggregationBuffer aggregationbuffer)
				throws HiveException {
			result.set(((CountAgg)aggregationbuffer).count);
			return result;
		}
		
		public class CountAgg implements AggregationBuffer{
			long count;
		}
		
	}
}
