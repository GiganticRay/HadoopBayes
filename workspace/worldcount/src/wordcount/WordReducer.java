package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


// 创建一个 WorldReducer 类继承与Reducer 抽象类
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();	// 记录词频
	
	// Reducer 抽象类的核心方法、3个参数
	public void reduce(Text key, 							// Map 端输出的 key
						Iterable<IntWritable> values,		// Map 端输出的 value 集合
						Context context)
						throws IOException, InterruptedException{
		
		int sum = 0;
		for (IntWritable val : values)	// 遍历 values 集合， 并把值相加
		{
			sum += val.get();
		}
		
		result.set(sum);			// 得到最终词频数
		context.write(key, result);	// 写入结果到 HDFS
	}

}
