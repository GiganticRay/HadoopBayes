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
import org.apache.hadoop.util.GenericOptionsParser;

import com.alibaba.fastjson.JSON;



public class NBMain
{
    
	public static void main(String[] args) throws Exception
	{	
		BayesPredict predictor = new BayesPredict(args);
		
		predictor.InitConfiguration();	// 初始化
//		predictor.GetFrequencyFile();	// 得到频数文件
		predictor.readMapFromHdfs("hdfs://node:9000/user/Hadoop/eclipseOutput2/part-r-00000");
		predictor.Predict();			// 开始预测
		predictor.GetAccurateFromPredictJob();	// 输出预测信息
	}

}
