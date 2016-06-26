package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.eps.movieRecommender.writable.JsonWritable;
import cat.eps.movieRecommender.writable.MovieWritable;

public class MoviesMapper  extends Mapper<LongWritable, Text,LongWritable,Text >
{

	private TreeMap<MovieWritable,Long> movies = new TreeMap<MovieWritable,Long>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		JsonWritable record = new JsonWritable(key,goodValues[1]);
		MovieWritable movie = new MovieWritable(goodValues[1],false);
		LongWritable rating = record.getRating();
		
		if(movies.containsKey(movie)){
			Long existentRatings = movies.get(movie);
			existentRatings+=rating.get();
			movies.replace(movie, movies.get(movie), existentRatings);
		}else{
			movies.put(movie, rating.get());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		NavigableSet<MovieWritable> nSet = movies.descendingKeySet();
		  	
		for (MovieWritable key: nSet) {
			Long overallRating = movies.get(key);
			key.setOverallRating(new LongWritable(overallRating));
			context.write(key.getMovieId(),new Text(key.toString()));
		}
	}
}
