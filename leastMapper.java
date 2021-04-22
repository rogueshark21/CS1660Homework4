import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class leastMapper extends Mapper<Object, Text, LongWritable, Text>
{
    // data format  => movie_name
    // no_of_views   (tab seperated)
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] tokens = value.toString().split("\t");

        String movie_name = tokens[0];
        long no_of_views = Long.parseLong(tokens[1]);

        no_of_views = no_of_views; // Remove the -1 from the example so we get the least

        context.write(new LongWritable(no_of_views), new Text(movie_name));
    }
}