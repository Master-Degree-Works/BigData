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

import cat.eps.movieRecommender.writable.JsonWritable;
import cat.eps.movieRecommender.writable.MovieWritable;

public class BestMoviesByZipMapper  extends Mapper<LongWritable, Text,Text,MovieWritable >
{

	private TreeMap<Text,List<MovieWritable>> moviesByZip = new TreeMap<Text,List<MovieWritable>>();
	private HashMap<LongWritable,Long> ocurrencesMap = new HashMap<LongWritable,Long>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		JsonWritable record = new JsonWritable(key,goodValues[1]);
		MovieWritable movie = new MovieWritable(goodValues[1],true);
		LongWritable rating = record.getRating();
		
		if(moviesByZip.containsKey(record.getUserZip())){
			moviesByZip.get(record.getUserZip()).add(movie);
		}else{
			moviesByZip.put(record.getUserZip(), new ArrayList<MovieWritable>());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		  	
		for (Text zipCode: moviesByZip.keySet()) {
			List<MovieWritable> moviesInZip = moviesByZip.get(zipCode);
			MovieWritable bestMovieForZip = moviesInZip.get(0);
			
			for(MovieWritable movieInZip : moviesInZip){
				if(movieInZip.getOverallRating().get()>bestMovieForZip.getOverallRating().get()){
					bestMovieForZip = movieInZip;
				}
			}
			
			context.write(zipCode,bestMovieForZip);
			
//			HashMap<MovieWritable,DoubleWritable> movieRatings = new HashMap<MovieWritable,DoubleWritable>();
//			for(MovieWritable movieInZip : moviesInZip){
//				if(movieRatings.containsKey(movieInZip)){
//					DoubleWritable actualRating = movieRatings.get(movieInZip);
////					DoubleWritable actualRating actualRating.get()= movieInZip.
//				}else{
//					movieRatings.put(movieInZip, movieInZip.getOverallRating());
//				}
//			}
		
		//	context.write(movieKey.getMovieId(),new Text(movieKey.toString()));
		}
	}
}
