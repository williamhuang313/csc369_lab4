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

    // Mapper for log file
    public static class ALMapper extends Mapper<Text, Text, Text, Text> {
	@Override
	    public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
        String[] sa = value.toString().split("\t");
        Text hostname = new Text();
	    hostname.set(sa[0]);
        Text count = new Text();
        count.set(sa[1] + "\tA");
        context.write(hostname, count);
	} 
    }

    // Mapper for country file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	    public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
        String[] sa = value.toString().split(",");
        String hostname = sa[0];
        String country = sa[1] + "\tB";
        context.write(new Text(hostname), new Text(country));
	}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

	@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
        
        String count = "";
        String country = "";

        for (Text val : values) {
            String[] value = val.toString().split("\t");
            if (value[1].equalsIgnoreCase("A")) {
                count = value[0];
            }
            else {
                country = value[0];
            }
        }

        context.write(new Text(country), new Text(count));
	    
	}
    } 


}
