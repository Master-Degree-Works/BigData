package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;

import cat.eps.movieRecommender.writable.UserWritable;

public class UsersStatsCalcMapper  extends Mapper<LongWritable, Text,Text,Text >
{

	private TreeMap<LongWritable,UserWritable> users = new TreeMap<LongWritable,UserWritable>();

	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		
		String[] goodValues = value.toString().split("\t");
		
		try {
			UserWritable user = new UserWritable(goodValues[1]);
			users.put(user.getUserId(),user);
		} catch (JSONException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		long mascGenre = 0l;
		long femGenre = 0l;

		long ageUnder18 = 0l;
		long age18_24 = 0l;
		long age25_34 = 0l;
		long age35_44 = 0l;
		long age45_49 = 0l;
		long age50_55 = 0l;
		long age56plus = 0l;

		/*  1:  "Under 18"
		 * 18:  "18-24"
		 * 25:  "25-34"
		 * 35:  "35-44"
		 * 45:  "45-49"
		 * 50:  "50-55"
		 * 56:  "56+"
		 */

		for (LongWritable userId: users.keySet()) {
						
			UserWritable user = users.get(userId);

			if(user.getUserGenre().toString().equals("M")){
				mascGenre++;
			}else{
				femGenre++;
			}

			switch (Integer.parseInt(String.valueOf(user.getUserAge().get()))) {
			case 1:
				ageUnder18++;
				break;
			case 18:
				age18_24++;
				break;
			case 25:
				age25_34++;
				break;
			case 35:
				age35_44++;
				break;
			case 45:
				age45_49++;
				break;
			case 50:
				age50_55++;
				break;
			case 56:
				age56plus++;
				break;

			default:
				break;
			}
		}

		context.write(new Text("Number of Fem Users"), new Text(String.valueOf(femGenre)));
		context.write(new Text("Number of Masc Users"), new Text(String.valueOf(mascGenre)));

		context.write(new Text("Age Under 18"), new Text(String.valueOf(ageUnder18)));
		context.write(new Text("Age Between 18 and 24"), new Text(String.valueOf(age18_24)));
		context.write(new Text("Age Between 25 and 34"), new Text(String.valueOf(age25_34)));
		context.write(new Text("Age Between 35 and 44"), new Text(String.valueOf(age35_44)));
		context.write(new Text("Age Between 45 and 49"), new Text(String.valueOf(age45_49)));
		context.write(new Text("Age Between 50 and 55"), new Text(String.valueOf(age50_55)));
		context.write(new Text("Age Plus 55"), new Text(String.valueOf(age56plus)));
	}
}
