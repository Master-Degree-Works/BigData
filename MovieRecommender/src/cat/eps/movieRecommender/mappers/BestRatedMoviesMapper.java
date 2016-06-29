package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cat.eps.movieRecommender.writable.MovieWritable;

public class BestRatedMoviesMapper  extends Mapper<LongWritable, Text,NullWritable,MovieWritable >
{

	private TreeMap<DoubleWritable,MovieWritable> topMovies = new TreeMap<DoubleWritable,MovieWritable>();

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {
		String minOcurrencesStr = context.getConfiguration().get("minOcurrences");
		Integer minOcurrences = new Integer(minOcurrencesStr);
		
		String[] goodValues = value.toString().split("\t");
		
		MovieWritable movie = new MovieWritable(goodValues[1],true);

		if(movie.getNumberOfOcurrences().get()>minOcurrences){
			topMovies.put(new DoubleWritable(movie.getOverallRating().get()),movie);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String nTop = context.getConfiguration().get("NTop");
		Integer nTopInt = new Integer(nTop);
		
		boolean allItems = (nTopInt ==999);
		NavigableSet<DoubleWritable> nSet = topMovies.descendingKeySet();
		  
		int itemCounter=0;
		
		TreeMap<DoubleWritable,MovieWritable> resultMap = new TreeMap<DoubleWritable,MovieWritable>();
		
		for (DoubleWritable key: nSet) {
			MovieWritable movie = topMovies.get(key);
			if(itemCounter>=nTopInt && !allItems){
				break;
			}

			resultMap.put(key, movie);

			itemCounter++;
		}
		
		for(Entry<DoubleWritable, MovieWritable> movieResult : resultMap.entrySet()){
			context.write(NullWritable.get(),movieResult.getValue());
		}		
		
	}
}
