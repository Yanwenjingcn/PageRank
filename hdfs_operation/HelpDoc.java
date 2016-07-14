package hdfs_operation;

import java.io.IOException;
import hdfs_operation.HDFSOperate;

import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class HelpDoc {
	private static final String HDFS = "hdfs://192.168.1.105:9000/";
	private static Configuration conf=new Configuration();
 
    public static ArrayList<String> getPages(String srcpath) throws IOException { //将text文件类型的中心点格式转换成double的格式，还是表格样子的呢//这么来设想：得到该文件目录下的所有文件的名字，如果名字是以part开头的，那么就把这么文件的名字作为最终的路径文件打开 
        ArrayList<String> result = new ArrayList<String>();  
        
        HDFSOperate dfs= new HDFSOperate(HDFS, conf);
        String inputPath=dfs.getFilePath(srcpath);//获得输入文件的绝对路径
        
        	FileSystem hdfs = FileSystem.get(URI.create(HDFS), conf);       	
            Path inPath = new Path(inputPath);  
            FSDataInputStream fsIn = hdfs.open(inPath);  
            LineReader lineIn = new LineReader(fsIn, conf);  
            Text line = new Text();           
            if(srcpath.endsWith("page")){
                while (lineIn.readLine(line) > 0) { //读取一行字符串 
                    String record = line.toString();  
                    result.add(record);  
                }     	
            }else{
                while (lineIn.readLine(line) > 0) { //读取一行字符串 
                    String record = line.toString();  
                    result.add(record);  
                    }                         
                }             
            fsIn.close(); 
         return result;  
    }  

//    public static boolean isFinished(String oldPath, String newPath, double threshold)//thershold阀值，k是有多少个初始中心点
//            throws IOException { //判断是否可以停止迭代了    
//        List<ArrayList<Double>> oldCenters = HelpDoc.getPages(oldPath);  
//        List<ArrayList<Double>> newCenters = HelpDoc.getPages(newPath);  
//        int c=oldCenters.size();//有多少个中心点 
//        int n=oldCenters.get(0).size();//得到每一个数据点的维数
//        float distance = 0;       
//        for (int i = 0; i < c; ++i){ //多少个中心点 
//            for (int j = 1; j < n; ++j){ //多少个维数 ，并计算新旧两个中心点的距离
//                double tmp = Math.abs(oldCenters.get(i).get(j) - newCenters.get(i).get(j));  
//                distance += Math.pow(tmp, 2);  
//            }  
//        }  
//        if (distance <= threshold)  
//            return true;          
//        return false;  
//    }  

}
