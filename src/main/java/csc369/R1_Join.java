package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class R1_Join {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class ALMapper extends Mapper<Text, Text, Text, Text> {
	@Override
	    public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
        String[] sa = value.toString().split("\\s+");
        Text hostname = new Text();
	    hostname.set(sa[0]);
        Text count = new Text();
        count.set(sa[1]);
        context.write(hostname, count);
	} 
    }

    // Mapper for messages file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	    public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
        String country = value.toString();
        context.write(key, new Text(country));
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        int counter = 0;
        Text[] values_list = {};
    
            
        for (Text val : values) {
            values_list[counter] = val;
            counter += 1;
        }
    
        context.write(values_list[0], values_list[1]);
	    
	}
    } 


}
