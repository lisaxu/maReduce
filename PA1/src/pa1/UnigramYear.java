package pa1;
 
import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 
public class UnigramYear {
    public static class UnigramYearMapper extends Mapper<NullWritable, BytesWritable, Text, ArrayWritable>{
        //produce <word, [bookID,year]>
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
         
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //context.write(word, one);
            }
        }
}
 
    public static void main(String[] args) {
        // TODO Auto-generated method stub
 
    }
 
}