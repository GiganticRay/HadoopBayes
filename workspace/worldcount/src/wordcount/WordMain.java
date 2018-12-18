package wordcount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class WordMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
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
		
		Job job = new Job(conf, "word count");	// 新建一个 Job，传入配置信息
		// region
		job.setJar("wordcount.jar");			// 设置运行的jar文件
		// endregion
		job.setJarByClass(WordMain.class);		// 设置主类
		job.setMapperClass(WordMapper.class);	// 设置 Mapper 类
		job.setCombinerClass(WordReducer.class);	// 设置作业合成类	(就是在 map 之后、reduce 之前、要进行一次)
		job.setReducerClass(WordReducer.class);	// 设置 Reducer 类
		job.setOutputKeyClass(Text.class);		// 设置输出数据的关键类
		job.setOutputValueClass(IntWritable.class);	// 设置输出值类
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));	// 文件输入
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));// 文件输出
		System.exit(job.waitForCompletion(true) ? 0 : 1);			// 等待完成退出
	}

}
