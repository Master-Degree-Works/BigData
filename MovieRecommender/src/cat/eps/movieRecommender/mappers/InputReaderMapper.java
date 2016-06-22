package cat.eps.movieRecommender.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class InputReaderMapper extends Mapper<LongWritable, Text, Text,Text >{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//User
		Long userId;
		Long userAge;
        String userGenre;
        Long userOccupation;
        String userZip;
		
        //Movie
        Long movieId;
        String movieTitle;
        String movieGenre;
        
        //Rating
        Integer rating;
        Long timestamp;
        
		String line = value.toString();
        String[] tuple = line.split("\\n");
         
         for(int i=0;i<tuple.length; i++){
             JSONObject obj;
			try {
				 obj = new JSONObject(tuple[i]);
				 
				 userId = obj.getLong("userId");
	             userGenre = obj.getString("userGenre");
	             userAge = obj.getLong("userAge");
	             userOccupation = obj.getLong("userOccupation");
	             userZip = obj.getString("userZip");
	             
	             movieId = obj.getLong("movieId");
	             movieTitle = obj.getString("movieTitle");
	             movieGenre = obj.getString("movieGenre");
	             
	             rating = obj.getInt("rating");
	             timestamp = obj.getLong("timestamp");
	             
	             
	             context.write(new Text(userId.toString()), new Text(userGenre));
			} catch (JSONException e) {
				e.printStackTrace();
			}   
         }
         
		//{"userId": 1503,"userGenre" : "M","userAge" : 25,"userOccupation" : 12,"userZip" : "92688","movieId" : 2926,"movieTitle" : "Hairspray (1988)","movieGenre" : "Comedy|Drama","rating" : 3,"timestamp" : "974770973","Date" : "Tue Nov 21 02:42:53 CET 2000"},
	}
}
