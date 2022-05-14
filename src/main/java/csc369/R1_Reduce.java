package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class R1_Reduce {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] sa = value.toString().split("\t");
        Text country = new Text();
	    country.set(sa[0]);
        IntWritable count = new IntWritable(sa[1]);
        context.write(country, count);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text country, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = counts.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(country, result);
       }
    }

}

