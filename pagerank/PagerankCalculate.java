package pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import hdfs_operation.HDFSOperate;
import hdfs_operation.HelpDoc;


public class PagerankCalculate {
	

	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		
		ArrayList<String> pages=null;
		int count_pages=0;
		protected void setup(Context context) throws IOException{
			pages= HelpDoc.getPages(context.getConfiguration().get("input_adjacency"));
			count_pages=pages.size();//中心点的总个数
		}
		private String flag;
        @Override
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        	FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的文件的文件夹名
            
        	String valueIn=values.toString();
        	if (flag.equals("adjacency")) { 
            	String row =valueIn.substring(0, 1);
            	String[] values_proba=valueIn.substring(2).split(","); 
            	for(int i=0;i<count_pages;i++){
            		String keyOut=String.valueOf(i+1);
            		String valueOut="proba:"+(row)+","+values_proba[i];
            		context.write(new Text(keyOut), new Text(valueOut));
            	}            	                  	
            }else if(flag.equals("PageRank_0")){
            	String[] values_pr=valueIn.split(",");
            	 for (int i = 1; i <= count_pages; i++) {
                     Text k = new Text(String.valueOf(i));
                     Text v = new Text("PR:" + values_pr[0] + "," + values_pr[1]);
                     context.write(k, v);
                 }
                }else{
                	String[] values_pr=valueIn.split("\t");
                	for (int i = 1; i <= count_pages; i++) {
                        Text k = new Text(String.valueOf(i));
                        Text v = new Text("PR:" + values_pr[0] + "," + values_pr[1]);
                        context.write(k, v);
                    } 	
                }
            }
        }


    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		ArrayList<String> pages=null;
		int count_pages=0;
		protected void setup(Context context) throws IOException{
			pages= HelpDoc.getPages(context.getConfiguration().get("input_adjacency"));
			count_pages=pages.size();//中心点的总个数
		}
    	@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Float> mapProba = new HashMap<Integer, Float>();
            Map<Integer, Float> mapPageRank = new HashMap<Integer, Float>();
            float float_PR = 0.0f;
            for (Text values_line : values) {//对map的结果进行整理。
                String  value= values_line.toString();
                if (value.startsWith("proba:")) {
                    String[] tokenA =value.substring(6).split(",");
                    mapProba.put(Integer.parseInt(tokenA[0]), Float.parseFloat(tokenA[1]));
                }
                if (value.startsWith("PR:")) {
                    String[] tokenB =value.substring(3).split(",");
                    mapPageRank.put(Integer.parseInt(tokenB[0]), Float.parseFloat(tokenB[1]));
                }
            }

            Iterator<Integer> iterA = mapProba.keySet().iterator();
            float damping =(float) 0.85;
            float additional=(float) ((1-damping)/count_pages);
            while(iterA.hasNext()){
                int index = (int) iterA.next();
                float probaValue = mapProba.get(index);//得到系数
                float PRValue = mapPageRank.get(index);//得到PR值
                float_PR += probaValue * PRValue;
            }
            float_PR=(float) (damping*float_PR)+additional;//乘以0.85的系数
            context.write(key, new Text(scaleFloat(float_PR)));
          
        }
        
        public static String scaleFloat(float f) {// 保留6位小数
            DecimalFormat df = new DecimalFormat("##0.000000");
            return df.format(f);
    }
        
    }

	public static void run(Map<String, String> path,int i) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		int count=0;
		Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "192.168.1.105:9001");
	    conf.set("fs.default.name","hdfs://192.168.1.105:9000");
	    
	    String input_adjacency = path.get("adjacency");//得到临接矩阵
	    conf.set("input_adjacency",input_adjacency);
	    
	    String input_pr=new String();
	    if(i==0){
	    	input_pr = path.get("input_pr"); //初始化pr值存储位置
	    }else{
	    	input_pr = path.get("PageRank_"+i);//上一次pr值存储的位置
	    }
	    
        count=i+1;
        String output_pr= path.get("PageRank_"+count);//临时的pr值存储
        HDFSOperate hdfs = new HDFSOperate(JobSet.HDFS, conf);
        hdfs.deleteFile(output_pr);

	    Job job = new Job(conf, "pr_PageRank_Test");
	    job.setJarByClass(PagerankCalculate.class);
	    job.setMapperClass(PageRankMapper.class);
	    job.setReducerClass(PageRankReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(job,  new Path(input_adjacency), new Path(input_pr));
		FileOutputFormat.setOutputPath(job, new Path(output_pr));
	    job.waitForCompletion(true);

	}
    }

