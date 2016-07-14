package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hdfs_operation.HDFSOperate;
import hdfs_operation.HelpDoc;

public class AdjacencyCalculate {
	
	 public static class AdjacencyMatrixMapper extends Mapper<Object, Text, Text, Text> {

		 public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
	         String valueIn=values.toString();
	         String[] tokens =valueIn.split(",");//分割原始输入的数据
	   		 int count =tokens.length;
	   		 StringBuilder valueOut = new StringBuilder();
	   		 Text keyOut=new Text(tokens[0]);//链出的网页编号
	   		 String String_valueOut=new String();
	   		 if(count==1){
	   			valueOut.append(" ");
	   			String_valueOut =valueOut.toString();
	   		 }
	   		 else{
	   		    for(int i=1;i<count;i++){
	   		    	valueOut.append(" "+tokens[i]);
	   		    }
	   		 String_valueOut =valueOut.toString().substring(1);	
	   		 }
	   		context.write(keyOut,new Text(String_valueOut));
	        }
	 }
	 
	 public static class AdjacencyMatrixReducer extends Reducer<Text, Text, Text, Text> {
		 
		 ArrayList<String> pages=null;
		 static int count_pages=0;
			protected void setup(Context context) throws IOException{
				pages= HelpDoc.getPages(context.getConfiguration().get("inputPages"));
				count_pages=pages.size();//中心点的总个数
			}  
			
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   
			 for(Text value:values){
				String valueIn=value.toString(); 
			    String[] tokens =valueIn.split(" ");
				float[] G = new float[count_pages];//创建矩阵的行
				int[] A = new int[count_pages];
				Arrays.fill(A,0);
				Arrays.fill(G,(float)0);//初始化数组为0.0
				int count = tokens.length;//链接的外出页面数	
				int countFlag=count;
				if(count==0)//没有链出的网页
					countFlag=1;
				else{	
				    for(int j=0;j<count;j++){//以该行为基础构建临界数组
		               int idx = Integer.parseInt(tokens[j]);//Text转换为字符串再转换成数组
		               A[idx-1] =1;
				}
				}
				float proba=(float)1.0/countFlag;//每个网页为其它网页的贡献值，概率：probability
				StringBuilder valueOut = new StringBuilder();
				String String_valueOut=new String();
				if(count!=0){
			        for (int i = 0; i < count_pages; i++) {
			        	if(A[i]==1)
			        		G[i]=proba;
			        	valueOut.append("," + (float)G[i]);
			            }      
				}else{
					for (int i = 0; i < count_pages; i++) {
						valueOut.append("," + (float)G[i]);
			            }
				}
				String_valueOut = new String(valueOut.toString().substring(1));
				context.write(key, new Text(String_valueOut));

	} 
	}
	}
	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub		
		    Configuration conf = new Configuration();//设置configue
		    conf.set("mapred.job.tracker", "192.168.1.105:9001");
		    conf.set("fs.default.name","hdfs://192.168.1.105:9000");

	        String input_page = path.get("input_page");//输入初始的页面
	        String adjacency = path.get("adjacency");//存放临界矩阵
	        conf.set("inputPages",input_page);
	        
	        HDFSOperate hdfs = new HDFSOperate(JobSet.HDFS, conf);
	        hdfs.deleteFile(adjacency);

		    Job job = new Job(conf, "pr_Adjacency_Test");
		    job.setJarByClass(AdjacencyCalculate.class);
		    job.setMapperClass(AdjacencyMatrixMapper.class);
		    job.setReducerClass(AdjacencyMatrixReducer.class);  
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(input_page));
		    FileOutputFormat.setOutputPath(job, new Path(adjacency));
		    job.waitForCompletion(true);
		  }

	}





