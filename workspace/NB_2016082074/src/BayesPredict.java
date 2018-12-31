import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.alibaba.fastjson.JSON;


public class BayesPredict
{		
	// Constructor Function
	public BayesPredict(String[] args){
		this.args = args;
		this.preciseCount = 0;
		this.allCount = 0;
		this.featureFrequency = new HashMap<String, Integer>();
		
	}

	// region define parameters
	public Configuration conf = null;					// 也可用 set 方法重新设置（会覆盖）: conf.set("fs.default.name", //"hdfs"//xxxx:9000) Configuration 类： 创建时会 | 读取 Hadoop 的配置文件，如 site-core.xml...;
	public double preciseCount;							// 当前准确条数
	public double allCount;								// 预测文件的总数
	public Map<String, Integer> featureFrequency;		// 频数MAP
	public String[] args;								// 命令行参数
	public String[] otherArgs = null;					// Other ARGS
	// end region
	
	// 初始化 conf
	public void InitConfiguration() throws IOException{
		if(conf == null){
			this.conf = new Configuration();
		}
		this.otherArgs = new GenericOptionsParser(this.conf, this.args).getRemainingArgs();	// 将命令行中参数自动设置到变量 conf 中
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
			System.err.println("Init confguration error: Usage: wordcount <in><out>");
			System.exit(2);
		}
		System.out.println("InitConfiguration successfully!");
	}
	
	
    // 从HDFS中读取数据、并写入MAP 存进 CONF global parameter 里面
    public void readMapFromHdfs(String fileName){
        Path filePath=new Path(fileName);
        try {
            FileSystem fs=FileSystem.get(URI.create(fileName), conf);
            if(fs.exists(filePath)){
                String charset="UTF-8";             
                FSDataInputStream fsDataInputStream=fs.open(filePath);							//打开文件数据输入流            
                InputStreamReader inputStreamReader=new InputStreamReader(fsDataInputStream,charset);	//创建文件输入
                String line=null;         
                BufferedReader reader=null;														//把数据读入到缓冲区中
                reader=new BufferedReader(inputStreamReader);
                while((line=reader.readLine())!=null){											//从缓冲区中读取数据
                    String[] lineContent = line.toString().split("\t");
        			this.featureFrequency.put(lineContent[0], Integer.parseInt(lineContent[1]));// 将其频数存放于全局变量
                }
            
                String featureFrequencyJson = JSON.toJSONString(featureFrequency);				// 序列化
        		conf.set("featureFrequencyJson", featureFrequencyJson);							// 将序列化的map存入全局变量
        		System.out.println("readMapFromHdfs Successfully!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    // 从job2的输出文件统计准确率
    public void CountAccurate(String fileName){
    	Path filePath=new Path(fileName);
        try {
            FileSystem fs = FileSystem.get(URI.create(fileName),conf);
            if(fs.exists(filePath)){
                String charset="UTF-8";             
                FSDataInputStream fsDataInputStream = fs.open(filePath);	//打开文件数据输入流            
                InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream,charset);	//创建文件输入
                String line=null;         
                BufferedReader reader=null;								//把数据读入到缓冲区中
                reader=new BufferedReader(inputStreamReader);
                while((line=reader.readLine())!=null){					//从缓冲区中读取数据
                	allCount ++;
                    String[] lineContent = line.toString().split("\t");
                    if(lineContent[1].equals(lineContent[2])){			// 说明预测准确
                    	preciseCount ++;
                    }
        			
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	
	// 训练, 得到统计频数文件
	public void GetFrequencyFile() throws Exception{
		Job trainJob = new Job(this.conf, "GetFrequencyFileJob");	// 新建一个 Job，传入配置信息	JobTrack 和 TaskTrack 都是 MRv1 用的东西了

		trainJob.setJarByClass(BayesClassifier.class);		// 设置主类
		trainJob.setMapperClass(BayesClassifier.TokenizerMapper.class);	// 设置 Mapper 类
		trainJob.setCombinerClass(BayesClassifier.IntSumReducer.class);	// 设置作业合成类	(就是在 map 之后、reduce 之前、要进行一次)
		trainJob.setReducerClass(BayesClassifier.IntSumReducer.class);	// 设置 Reducer 类
		trainJob.setOutputKeyClass(Text.class);			// 设置输出数据的关键类
		trainJob.setOutputValueClass(IntWritable.class);	// 设置输出值类
		
		// Train model, 就是统计每一个 特征属性 好评、差评 的频率
		FileInputFormat.addInputPath(trainJob, new Path(this.otherArgs[0]));	// 文件输入
		FileOutputFormat.setOutputPath(trainJob, new Path(this.otherArgs[1]));// 文件输出
		
		boolean flag = trainJob.waitForCompletion(true);
		System.out.print("GetFrequencyFileJob: " + flag);
	}
	
	
	// 预测文件 JOB、输出预测的文件
	public void Predict() throws Exception{
		// countFrequencyJob: 判断输入文件中每一行的值
		Job predictJob = new Job(this.conf, "countFrequencyJob");
		predictJob.setJarByClass(BayesClassifier.class);		// 设置主类
		predictJob.setMapperClass(BayesClassifier.BayesPredictMapper.class);	// 设置 Mapper 类
		predictJob.setNumReduceTasks(0);   	 			//reduce的数量设为0
		
		FileInputFormat.addInputPath(predictJob, new Path("hdfs://192.168.10.100:9000/user/Hadoop/BayesData/test-1000.txt"));	// 文件输入
		FileOutputFormat.setOutputPath(predictJob, new Path("hdfs://192.168.10.100:9000/user/Hadoop/eclipseOutput4"));// 文件输出
		boolean flag = predictJob.waitForCompletion(true);
		System.out.println("GetFrequencyMap: " + flag);
	}
	
	
	// 根据 job2 的输出文件来统计 accurate rate
	public void GetAccurateFromPredictJob(){
		CountAccurate("hdfs://node:9000/user/Hadoop/eclipseOutput4/part-m-00000");
		System.out.println("The accurate of this prediction is : " + this.preciseCount/this.allCount);
	}

}
