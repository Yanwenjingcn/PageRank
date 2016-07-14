package hdfs_operation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSOperate {
	    private static final String HDFS = "hdfs://192.168.1.105:9000/";
	    private String hdfsPath;
	    private Configuration conf=new Configuration();
	    public HDFSOperate(Configuration conf) {
	        this(HDFS, conf);
	    }
	    public HDFSOperate(String hdfs, Configuration conf) {
	        this.hdfsPath = hdfs;
	        this.conf = conf;
	    }
 
	    public void mkdirs(String folder) throws IOException {
	        Path path = new Path(folder);
	        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
	        if (!fs.exists(path)) {
	            fs.mkdirs(path);
	        }
	        fs.close();
	    }

	    public void deleteFile(String folder) throws IOException {
	        Path path = new Path(folder);
	        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
	        fs.deleteOnExit(path);
	        fs.close();
	    }

	    public void createFile(String file, String content) throws IOException {
	        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
	        byte[] buff = content.getBytes();
	        FSDataOutputStream os = null;
	        try {
	            os = fs.create(new Path(file));
	            os.write(buff, 0, buff.length);
	            System.out.println("Create: " + file);
	        } finally {
	            if (os != null)
	                os.close();
	        }
	        fs.close();
	    }

	    public void copyFile(String local, String remote) throws IOException {
	        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
	        fs.copyFromLocalFile(new Path(local), new Path(remote));
	        fs.close();
	    }

	    public void renameFile(String source,String des) throws IOException {
	    	FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
	        Path frpaht=new Path(source);    //旧的文件名
	        Path topath=new Path(des);    //新的文件名
	        fs.rename(frpaht, topath);
	        fs.close();
	    }
	    
	    public  String getFilePath(String srcpath) throws IOException{
	        FileSystem hdfs = FileSystem.get(URI.create(hdfsPath), conf);
	       	 Path s_path = new Path(srcpath);
	       	 String despath=srcpath;//源路径
	       	 String path_find=srcpath;
	       		FileStatus stats[]=hdfs.listStatus(s_path);   		
	       	    for(int i = 0; i < stats.length; i++){
	       	        	String n=stats[i].getPath().toString();//这个路径是从hdfs://192.1。。开始的啊
	       	        	if((n.startsWith(despath+"/p"))){
	       	        		path_find=n;	
	       	        	}
	       	    	}
	       	return path_find;
	       	}

}
