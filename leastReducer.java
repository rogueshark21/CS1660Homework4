
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class leastReducer extends Reducer<LongWritable, Text, LongWritable, Text>
{
    static int count;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        count = 0;
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        // key                  values
        //-ve of no_of_views    [ movie_name ..]
        long no_of_views = key.get(); // Remove the -1 from the example so we get the least
        String movie_name = null;

        for (Text val : values)
        {
            movie_name = val.toString();
        }

        // we just write 5 records as output
        if (count < 5)
        {
            context.write(new LongWritable(no_of_views), new Text(movie_name));
            count++;
        }
    }
}