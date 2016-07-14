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
 
    public static ArrayList<String> getPages(String srcpath) throws IOException { //��text�ļ����͵����ĵ��ʽת����double�ĸ�ʽ�����Ǳ�����ӵ���//��ô�����룺�õ����ļ�Ŀ¼�µ������ļ������֣������������part��ͷ�ģ���ô�Ͱ���ô�ļ���������Ϊ���յ�·���ļ��� 
        ArrayList<String> result = new ArrayList<String>();  
        
        HDFSOperate dfs= new HDFSOperate(HDFS, conf);
        String inputPath=dfs.getFilePath(srcpath);//��������ļ��ľ���·��
        
        	FileSystem hdfs = FileSystem.get(URI.create(HDFS), conf);       	
            Path inPath = new Path(inputPath);  
            FSDataInputStream fsIn = hdfs.open(inPath);  
            LineReader lineIn = new LineReader(fsIn, conf);  
            Text line = new Text();           
            if(srcpath.endsWith("page")){
                while (lineIn.readLine(line) > 0) { //��ȡһ���ַ��� 
                    String record = line.toString();  
                    result.add(record);  
                }     	
            }else{
                while (lineIn.readLine(line) > 0) { //��ȡһ���ַ��� 
                    String record = line.toString();  
                    result.add(record);  
                    }                         
                }             
            fsIn.close(); 
         return result;  
    }  

//    public static boolean isFinished(String oldPath, String newPath, double threshold)//thershold��ֵ��k���ж��ٸ���ʼ���ĵ�
//            throws IOException { //�ж��Ƿ����ֹͣ������    
//        List<ArrayList<Double>> oldCenters = HelpDoc.getPages(oldPath);  
//        List<ArrayList<Double>> newCenters = HelpDoc.getPages(newPath);  
//        int c=oldCenters.size();//�ж��ٸ����ĵ� 
//        int n=oldCenters.get(0).size();//�õ�ÿһ�����ݵ��ά��
//        float distance = 0;       
//        for (int i = 0; i < c; ++i){ //���ٸ����ĵ� 
//            for (int j = 1; j < n; ++j){ //���ٸ�ά�� ���������¾��������ĵ�ľ���
//                double tmp = Math.abs(oldCenters.get(i).get(j) - newCenters.get(i).get(j));  
//                distance += Math.pow(tmp, 2);  
//            }  
//        }  
//        if (distance <= threshold)  
//            return true;          
//        return false;  
//    }  

}
