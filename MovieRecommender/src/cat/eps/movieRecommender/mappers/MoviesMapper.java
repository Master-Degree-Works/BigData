package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;

import cat.eps.movieRecommender.writable.JsonWritable;
import cat.eps.movieRecommender.writable.MovieWritable;

public class MoviesMapper  extends Mapper<LongWritable, Text,LongWritable,Text >
{

	private TreeMap<MovieWritable,Long> movies = new TreeMap<MovieWritable,Long>();
	private HashMap<LongWritable,Long> ocurrencesMap = new HashMap<LongWritable,Long>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		JsonWritable record;
		try {
			record = new JsonWritable(key,goodValues[1]);
			MovieWritable movie = new MovieWritable(goodValues[1],false);
			LongWritable rating = record.getRating();
			
			if(movies.containsKey(movie)){
				Long existentRatings = movies.get(movie);
				existentRatings+=rating.get();

				ocurrencesMap.replace(movie.getMovieId(),ocurrencesMap.get(movie.getMovieId()),ocurrencesMap.get(movie.getMovieId())+1);

				movies.replace(movie, movies.get(movie), existentRatings);
			}else{
				ocurrencesMap.put(movie.getMovieId(), 1l);
				movies.put(movie, rating.get());
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		NavigableSet<MovieWritable> nSet = movies.descendingKeySet();
		  	
		for (MovieWritable movieKey: nSet) {
			Long overallRating = movies.get(movieKey);
			Long numberOcurrences = ocurrencesMap.get(movieKey.getMovieId());
			movieKey.setNumberOfOcurrences(new LongWritable(numberOcurrences));
			movieKey.setOverallRating(new DoubleWritable((double)overallRating/(double)numberOcurrences));
			context.write(movieKey.getMovieId(),new Text(movieKey.toString()));
		}
	}
}
