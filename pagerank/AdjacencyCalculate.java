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
	         String[] tokens =valueIn.split(",");//�ָ�ԭʼ���������
	   		 int count =tokens.length;
	   		 StringBuilder valueOut = new StringBuilder();
	   		 Text keyOut=new Text(tokens[0]);//��������ҳ���
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
				count_pages=pages.size();//���ĵ���ܸ���
			}  
			
		 @Override
		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   
			 for(Text value:values){
				String valueIn=value.toString(); 
			    String[] tokens =valueIn.split(" ");
				float[] G = new float[count_pages];//�����������
				int[] A = new int[count_pages];
				Arrays.fill(A,0);
				Arrays.fill(G,(float)0);//��ʼ������Ϊ0.0
				int count = tokens.length;//���ӵ����ҳ����	
				int countFlag=count;
				if(count==0)//û����������ҳ
					countFlag=1;
				else{	
				    for(int j=0;j<count;j++){//�Ը���Ϊ���������ٽ�����
		               int idx = Integer.parseInt(tokens[j]);//Textת��Ϊ�ַ�����ת��������
		               A[idx-1] =1;
				}
				}
				float proba=(float)1.0/countFlag;//ÿ����ҳΪ������ҳ�Ĺ���ֵ�����ʣ�probability
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
		    Configuration conf = new Configuration();//����configue
		    conf.set("mapred.job.tracker", "192.168.1.105:9001");
		    conf.set("fs.default.name","hdfs://192.168.1.105:9000");

	        String input_page = path.get("input_page");//�����ʼ��ҳ��
	        String adjacency = path.get("adjacency");//����ٽ����
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





