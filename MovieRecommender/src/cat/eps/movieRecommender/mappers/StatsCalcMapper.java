package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.eps.movieRecommender.writable.MovieWritable;

public class StatsCalcMapper  extends Mapper<LongWritable, Text,Text,Text >
{

	private TreeMap<DoubleWritable,MovieWritable> topMovies = new TreeMap<DoubleWritable,MovieWritable>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String minOcurrencesStr = context.getConfiguration().get("minOcurrences");
		Integer minOcurrences = new Integer(minOcurrencesStr);
		
		String[] goodValues = value.toString().split("\t");
		
		MovieWritable movie;
		
			movie = new MovieWritable(goodValues[1],true);
			if(movie.getNumberOfOcurrences().get()>minOcurrences){
				topMovies.put(new DoubleWritable(movie.getOverallRating().get()),movie);
			}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		  	
		NavigableSet<DoubleWritable> nSet = topMovies.descendingKeySet();
		  
	
		
		MovieWritable maxOcurrencesMovie= null;
		double averageRating = 0l;
		double allRatings = 0l;
				
		for (DoubleWritable key: nSet) {
			MovieWritable movie = topMovies.get(key);
			
			allRatings+=movie.getOverallRating().get();
			
			if(maxOcurrencesMovie==null){
				maxOcurrencesMovie = movie;
			}else if(maxOcurrencesMovie.getNumberOfOcurrences().get()<movie.getNumberOfOcurrences().get()){
				maxOcurrencesMovie = movie;
			}
		}
		
		context.write(new Text("Most times rated Movie"), new Text(maxOcurrencesMovie.toStringSimple()));
			
		averageRating=allRatings/topMovies.keySet().size();
		context.write(new Text("Average Rating"), new Text(String.valueOf(averageRating)));
	}
}
