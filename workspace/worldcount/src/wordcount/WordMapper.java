package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// 创建一个 WordMapper 类继承于 Mapper 抽象类
public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// Mapeer 抽象类的核心方法、3个参数
	public void map(Object key,			// 首字符偏移量
					Text value, 		// 文件的一行内容
					Context context)	// Mapper 端的上下文，与 OutputCollector 和 Reporter 的功能类似
					throws IOException, InterruptedException{
		
		// Tokenizer: 分词器
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens())
		{
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
