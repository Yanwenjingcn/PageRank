package pagerank;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import hdfs_operation.HDFSOperate;

public class JobSet {
	public static final String HDFS = "hdfs://192.168.1.105:9000";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int count=0;
		Configuration conf = new Configuration();
	    conf.set("mapred.job.tracker", "192.168.1.105:9001");
	    conf.set("fs.default.name","hdfs://192.168.1.105:9000");
		Map<String, String> path = new HashMap<String, String>();
		path.put("input", HDFS + "/user/hduser/pagerank");// HDFS的总目录
	    path.put("input_page", HDFS + "/user/hduser/pagerank/page");// 初始PR值存放目录位置
	    path.put("adjacency", HDFS + "/user/hduser/pagerank/adjacency");// 临时目录,存放计算出来的邻接矩阵
	    path.put("input_pr", HDFS + "/user/hduser/pagerank/PageRank_0");// 计算结果的PR
	    try {
	    	AdjacencyCalculate.run(path);
            int iter = 30;//迭代的次数
            for (int i = 0; i < iter; i++) {// 迭代执行
            	count=i+1;
            	path.put("PageRank_"+count, HDFS+"/user/hduser/pagerank/PageRank_"+count);
            	HDFSOperate hdfs = new HDFSOperate(JobSet.HDFS, conf);
    	        hdfs.deleteFile("PageRank_"+count);
            	PagerankCalculate.run(path,i);//迭代运行每一次的计算。
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
