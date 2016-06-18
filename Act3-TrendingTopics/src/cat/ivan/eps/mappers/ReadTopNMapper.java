package cat.ivan.eps.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadTopNMapper extends Mapper<LongWritable, Text, Text,Text >{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
			
		context.write(new Text(), new Text("A"+value.toString().split("\t")[0]));
	}
}
