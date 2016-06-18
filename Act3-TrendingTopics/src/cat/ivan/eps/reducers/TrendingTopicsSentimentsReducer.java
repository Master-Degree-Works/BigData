package cat.ivan.eps.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cat.ivan.eps.writable.TweetWritable;

public class TrendingTopicsSentimentsReducer extends Reducer<Text, TweetWritable, Text, TweetWritable> {


	@Override
	protected void reduce(Text key, Iterable<TweetWritable> values, Reducer<Text, TweetWritable, Text, TweetWritable>.Context context) throws IOException, InterruptedException {

		for (TweetWritable value : values) {
			if(!value.getTweetId().toString().isEmpty()){
				context.write(key, value);
			}
		}
	}




}
