package cc.home.count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable lineNum, Text line, Context context) throws IOException, InterruptedException {
        System.out.println("Reading line : " + line);

        for( String word : line.toString().split(" ") ){
            context.write( new Text(word) , new IntWritable(1));
        }
    }
}
