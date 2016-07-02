package cat.eps.movieRecommender.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class InputReaderMapper extends Mapper<LongWritable, Text,LongWritable ,Text >{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] tuple = line.split("\\n");

		for(int i=0;i<tuple.length; i++){
			JSONObject obj;
			try {
				obj = new JSONObject(tuple[i]);           
				context.write(key,new Text(obj.toString()));
			} catch (JSONException e) {
				e.printStackTrace();
			}   
		}       
	}
}
