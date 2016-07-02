package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.eps.movieRecommender.writable.UserWritable;

public class UsersMapper  extends Mapper<LongWritable, Text,LongWritable,Text >
{


	private HashMap<LongWritable,Long> ocurrencesMap = new HashMap<LongWritable,Long>();
	private HashMap<LongWritable,UserWritable> usersMap = new HashMap<LongWritable,UserWritable>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");


		UserWritable user = new UserWritable(goodValues[1],false);

		if(ocurrencesMap.containsKey(user)){
			ocurrencesMap.replace(user.getUserId(),ocurrencesMap.get(user.getUserId()),ocurrencesMap.get(user.getUserId())+1);		
		}else{
			ocurrencesMap.put(user.getUserId(), 1l);
		}

		if(!usersMap.containsKey(user.getUserId())){
			usersMap.put(user.getUserId(), user);
		}



	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		for (LongWritable userId: ocurrencesMap.keySet()) {
			Long numberOcurrences = ocurrencesMap.get(userId);
			usersMap.get(userId).setUserOcurrences(new LongWritable(numberOcurrences));
			context.write(userId,new Text(usersMap.get(userId).toStringSimple()));
		}
	}
}
