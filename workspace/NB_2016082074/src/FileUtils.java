import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileUtils
{
	/*
     * 往hdfs中写数据
     */
    public static void writeToHdfs(String filename,String text){
        Configuration configuration=new Configuration();
        FSDataOutputStream out=null;
        String charset="UTF-8";
        try {
            FileSystem fSystem=FileSystem.get(URI.create(filename),configuration);
            Path path=new Path(filename);
            if(!fSystem.exists(path)){
                //创建文件数据的输出流
                out=fSystem.create(new Path(filename));
                //通过输出流往hdfs中写入数据
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }else{
                //往文件中追加数据
                out=fSystem.append(path);
                out.write(text.getBytes(charset),0,text.getBytes(charset).length);
                out.write("\n".getBytes(charset),0,"\n".getBytes(charset).length);
                out.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            //关闭输出流
            if(out!=null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
