import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class R1_Sort {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] sa = value.toString().split("\t");
        Text country = new Text();
	    country.set(sa[0]);
        IntWritable count = new IntWritable(Integer.parseInt(sa[1]) * -1);
        context.write(count, country);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
    
        @Override
	protected void reduce(IntWritable count, Iterable<Text> countries, Context context) throws IOException, InterruptedException {
        Integer true_count = count.get();
        
        for (Text country : countries) {
            IntWritable int_count = new IntWritable(true_count * -1);
            context.write(int_count, country);
        }
    }
}
}
