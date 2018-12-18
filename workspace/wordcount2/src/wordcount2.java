import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class wordcount2
{

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		// inputformat: (0, I LOVE YOU, context)
		public void map(Object key,			// 首字符偏移量
				Text value, 		// 文件的一行内容
				Context context)	// Mapper 端的上下文，与 OutputCollector 和 Reporter 的功能类似
				throws IOException, InterruptedException{
			// Tokenizer: 分词器
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);	// outputformat: (I, {1}), (LOVE, {1}), (YOU, {1}), 之后要经过 一次map后的 shuffle 操作， 包括（sort and combine）
											// 产生多个spill文件
			}
		}
	}
	
	// Reducer 前还有一次 shuffle 操作、就是(merge sort)
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
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
	
	public static class BayesPredict{
		
		// 训练
		public void train(String trainingData, String modelFile){
			// 特征提取
			// 模型训练
			// get a model
		}
		
		// 预测
		public String predict(String sentence, String modelFile){
			// 特征提取
			// 基于训练好的模型实现预测
			return "True";
		}
		
		// 验证集
		public void validate(String trainingDataFile, String sentencesFile, String modelFile, String resultFile){
			
		}
		
		// 加载模型
		public void load(String modelFile){
			
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
		// Configuration 类： 创建时会 读取 Hadoop 的配置文件，如 site-core.xml...;
			// 也可用 set 方法重新设置（会覆盖）: conf.set("fs.default.name", //"hdfs"//xxxx:9000)
			Configuration conf = new Configuration();
			
			// 将命令行中参数自动设置到变量 conf 中
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			// region 以下为 eclopse 插件提交所添加的代码、因为之前配置的hadoopLocation 并没有完全起作用、eclipse 获取不到集群环境下的配置信息，导致提交任务是加载的配置信息为默认值
			conf.set("fs.defaultFS", "hdfs://192.168.10.100:9000");
			conf.set("hadoop.job.user", "ray");
			conf.set("mapreduce.jobtracker.address", "192.168.10.100:9001");
			conf.set("yarn.resourcemanager.hostname", "192.168.10.100");
			conf.set("yarn.resourcemanager.admin.address", "192.168.10.100:8033");
			conf.set("yarn.resourcemanager.address", "192.168.10.100:8032");
			conf.set("yarn.resourcemanager.scheduler.address", "192.168.10.100:8030");
			// endregion
			
			if(otherArgs.length != 2){
				System.err.println("Usage: wordcount <in><out>");
				System.exit(2);
			}
			
			Job job = new Job(conf, "word count2");	// 新建一个 Job，传入配置信息	JobTrack 和 TaskTrack 都是 MRv1 用的东西了
			// region
			job.setJar("wordcount2.jar");			// 设置运行的jar文件
			// endregion
			job.setJarByClass(wordcount2.class);		// 设置主类
			job.setMapperClass(TokenizerMapper.class);	// 设置 Mapper 类
			job.setCombinerClass(IntSumReducer.class);	// 设置作业合成类	(就是在 map 之后、reduce 之前、要进行一次)
			job.setReducerClass(IntSumReducer.class);	// 设置 Reducer 类
			job.setOutputKeyClass(Text.class);			// 设置输出数据的关键类
			job.setOutputValueClass(IntWritable.class);	// 设置输出值类
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));	// 文件输入
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 文件输出
			
			boolean flag = job.waitForCompletion(true);
			System.out.print("SUCCEED!" + flag);
			System.exit(flag ? 0 : 1);			// 等待完成退出
			System.out.println();
	}

}
