package cat.ivan.eps.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cat.ivan.eps.utils.TweetUtils;
import cat.ivan.eps.writable.TweetWritable;

public class GathererReducer extends Reducer<Text, Text, Text, TweetWritable> {

	List<String> topNHashTags = new ArrayList<String>();
	List<String> tweets = new ArrayList<String>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


		for(Text value : values){
			if (value.charAt(0) == 'A') {
				String textWithoutA = value.toString().substring(1);
				topNHashTags.add(textWithoutA.substring(textWithoutA.toString().indexOf("#")).toLowerCase().trim()); 
			}
			else{
				tweets.add(value.toString());
			}
		}
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, TweetWritable>.Context context)
			throws IOException, InterruptedException {
		doLogic(context);
	}

	private void doLogic(Reducer<Text, Text, Text, TweetWritable>.Context context) throws IOException, InterruptedException {

		for(String tweetStr : tweets){
			TweetWritable tweet =  TweetUtils.buildTweetWritable(tweetStr);
			String[] splittedHastags = tweet.getHashtag().toString().trim().split("(\\s+)");

			for(String hashtag : splittedHastags){
				int hashPosition = hashtag.indexOf("#");
				if(hashPosition>=0){
					hashtag = hashtag.substring(hashPosition);
					if(topNHashTags.contains(hashtag)){
						context.write(new Text(""), tweet);
						break;
					}
				}
			}
		}	
	}
}