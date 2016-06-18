package cat.ivan.eps.reducers;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cat.ivan.eps.writable.TweetWritable;

public class TrendingTopicsFieldSelectionReducer extends Reducer<Text, TweetWritable, Text, TweetWritable> {

	@Override
	protected void reduce(Text key, Iterable<TweetWritable> values, Reducer<Text, TweetWritable, Text, TweetWritable>.Context context) throws IOException, InterruptedException {
		for (TweetWritable value : values) {
			context.write(new Text(""), value);
		}
	}
}
