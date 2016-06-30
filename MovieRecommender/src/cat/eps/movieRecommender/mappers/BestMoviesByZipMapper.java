package cat.eps.movieRecommender.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

	public void map(LongWritable key,Text  value, Context context) throws IOException, InterruptedException {

		String[] goodValues = value.toString().split("\t");
		
		JsonWritable record;
		try {
			record = new JsonWritable(key,goodValues[1]);
			MovieWritable movie = new MovieWritable(goodValues[1],true);
			
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
		  	
		for (Text zipCode: moviesByZip.keySet()) {
			List<MovieWritable> moviesInZip = moviesByZip.get(zipCode);
						
			for(MovieWritable movieInZip : moviesInZip){
				movieInZip.setNumberOfOcurrences(new LongWritable(0));
				movieInZip.setOverallRating(new DoubleWritable(0));
				context.write(new Text("A###"+zipCode),new Text(movieInZip.toString()));
//				context.write(new Text(zipCode),new Text(movieInZip.toString()));
			}
			
			
		}
	}
}
