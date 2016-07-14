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
		path.put("input", HDFS + "/user/hduser/pagerank");// HDFS����Ŀ¼
	    path.put("input_page", HDFS + "/user/hduser/pagerank/page");// ��ʼPRֵ���Ŀ¼λ��
	    path.put("adjacency", HDFS + "/user/hduser/pagerank/adjacency");// ��ʱĿ¼,��ż���������ڽӾ���
	    path.put("input_pr", HDFS + "/user/hduser/pagerank/PageRank_0");// ��������PR
	    try {
	    	AdjacencyCalculate.run(path);
            int iter = 30;//�����Ĵ���
            for (int i = 0; i < iter; i++) {// ����ִ��
            	count=i+1;
            	path.put("PageRank_"+count, HDFS+"/user/hduser/pagerank/PageRank_"+count);
            	HDFSOperate hdfs = new HDFSOperate(JobSet.HDFS, conf);
    	        hdfs.deleteFile("PageRank_"+count);
            	PagerankCalculate.run(path,i);//��������ÿһ�εļ��㡣
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
