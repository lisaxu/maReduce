package pa1;
 
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



 
public class UnigramYear {
    public static class UnigramYearMapper extends Mapper<Text, BytesWritable, Text, Text>{
        //produce <word, [bookID,year]>
        private final static IntWritable one = new IntWritable(1);
        private Text unigram = new Text();
  
        
    	public String getAuthor(String wholeFile) {
    		int start = wholeFile.indexOf("Author: ");
    		int end = wholeFile.indexOf('\n', start);
    		start += wholeFile.substring(start, end).lastIndexOf(' ') + 1;
    		return wholeFile.substring(start, end - 1);
    	}
    	
        public String getYear(String wholeFile) {
        	int start = wholeFile.indexOf("Release");
        	start = wholeFile.indexOf(',', start);
    		return wholeFile.substring(start+ 2, start + 6);
    	}
        public String getBookID(String wholeFile) {
        	int start = wholeFile.indexOf("[EBook");
        	int end = wholeFile.indexOf(']', start);
        	return wholeFile.substring(start + 8,end);
    	}
     
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String wholeFile = new String(value.getBytes());
            String author = getAuthor(wholeFile);
            String year = getYear(wholeFile);
            String id = getBookID(wholeFile);
            String info =  year + " " + id + " " + author;
            Text valueArray = new Text();
            valueArray.set(info);
            
            context.write(new Text(id), valueArray);
        }
    }
    
    public static class UnigramYearReducer extends Reducer<Text,Text,Text,IntWritable> {
    		private IntWritable result = new IntWritable();

    		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    			//int sum = 0;
    			result.set(2);
    			for (Text val : values) {
    				context.write(val, result);
    			}    			
    		}
    }

 
    public static void main(String[] args) throws Exception {
    	if (args.length != 2) {
    	      System.out.printf("Usage: Unigram (Year) <input dir> <output dir>\n");
    	      System.exit(-1);
    	    }

    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unigram year");
        job.setJarByClass(UnigramYear.class);
        
        job.setInputFormatClass(WholeFileInput.class);
        //set mapper and reducer
        job.setMapperClass(UnigramYearMapper.class);
        job.setReducerClass(UnigramYearReducer.class);
       
        
        //set input path
    	WholeFileInput.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	//set map output K,V
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);

    	boolean success = job.waitForCompletion(true);
    	System.exit(success ? 0 : 1);
 
    }


	
}