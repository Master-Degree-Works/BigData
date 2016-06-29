package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;

import cat.eps.movieRecommender.writable.JsonWritable;
import cat.eps.movieRecommender.writable.MovieWritable;

public class BestMoviesByZipMapper  extends Mapper<LongWritable, Text,Text,Text >
{

	private TreeMap<Text,List<MovieWritable>> moviesByZip = new TreeMap<Text,List<MovieWritable>>();
	private HashMap<LongWritable,Long> ocurrencesMap = new HashMap<LongWritable,Long>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		JsonWritable record;
		try {
			record = new JsonWritable(key,goodValues[1]);
			MovieWritable movie = new MovieWritable(goodValues[1],true);
			LongWritable rating = record.getRating();
			
			if(!moviesByZip.isEmpty() && moviesByZip.containsKey(record.getUserZip())){
				moviesByZip.get(record.getUserZip()).add(movie);
			}else{
				moviesByZip.put(record.getUserZip(), new ArrayList<MovieWritable>());
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		  	
//		for (Text zipCode: moviesByZip.keySet()) {
//			List<MovieWritable> moviesInZip = moviesByZip.get(zipCode);
//			MovieWritable bestMovieForZip = moviesInZip.get(0);
//			
//			for(MovieWritable movieInZip : moviesInZip){
//				if(movieInZip.getOverallRating().get()>bestMovieForZip.getOverallRating().get()){
//					bestMovieForZip = movieInZip;
//				}
//			}
//			
//			context.write(zipCode,bestMovieForZip);
//		}
		for (Text zipCode: moviesByZip.keySet()) {
			List<MovieWritable> moviesInZip = moviesByZip.get(zipCode);
						
			for(MovieWritable movieInZip : moviesInZip){
				context.write(new Text("A"+zipCode),new Text(movieInZip.toString()));
			}
			
			
		}
	}
}
