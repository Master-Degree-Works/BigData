package cat.eps.movieRecommender.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class MovieWritable implements WritableComparable<Object> {


	//Movie
	LongWritable movieId;
	Text movieTitle;
	Text movieGenre;
	
	LongWritable overallRating;


	public MovieWritable(Text key) {
		
		try {
			JSONObject obj = new JSONObject(key.toString());
			
			this.movieId=new LongWritable(obj.getLong("movieId"));
			this.movieTitle=new Text(obj.getString("movieTitle"));
			this.movieGenre=new Text(obj.getString("movieGenre"));
			this.overallRating = new LongWritable(obj.getLong("rating"));
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public MovieWritable() {
		
		this.movieId=new LongWritable();
		this.movieTitle=new Text("");
		this.movieGenre=new Text("");
		this.overallRating = new LongWritable();
	}

	public MovieWritable(JSONObject obj) {

		try {
		
			this.movieId=new LongWritable(obj.getLong("movieId"));
			this.movieTitle=new Text(obj.getString("movieTitle"));
			this.movieGenre=new Text(obj.getString("movieGenre"));
			this.overallRating = new LongWritable(obj.getLong("rating"));

		} catch (JSONException e) {
			e.printStackTrace();
		}
	}


	


	public MovieWritable(String string, boolean addRatings) {
		try {
			JSONObject obj = new JSONObject(string);
			
			this.movieId=new LongWritable(obj.getLong("movieId"));
			this.movieTitle=new Text(obj.getString("movieTitle"));
			this.movieGenre=new Text(obj.getString("movieGenre"));
			if(addRatings){
				this.overallRating = new LongWritable(obj.getLong("rating"));
			}else{
				this.overallRating = new LongWritable(0);
			}
			
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	
	
	@Override
	public String toString() {
		StringBuilder strBOut = new StringBuilder();
		strBOut.append("{");
		strBOut.append("\"movieId\":"+this.getMovieId()+",");
		strBOut.append("\"movieTitle\":\""+ this.getMovieTitle().toString()+"\",");
		strBOut.append("\"movieGenre\":\""+this.getMovieGenre().toString()+"\",");
		strBOut.append("\"rating\":"+ this.overallRating.toString());
		strBOut.append("},");
		
		return strBOut.toString();
	}

	public LongWritable getOverallRating() {
		return overallRating;
	}

	public void setOverallRating(LongWritable overallRating) {
		this.overallRating = overallRating;
	}

	public LongWritable getMovieId() {
		return movieId;
	}

	public void setMovieId(LongWritable movieId) {
		this.movieId = movieId;
	}

	public Text getMovieTitle() {
		return movieTitle;
	}

	public void setMovieTitle(Text movieTitle) {
		this.movieTitle = movieTitle;
	}

	public Text getMovieGenre() {
		return movieGenre;
	}

	public void setMovieGenre(Text movieGenre) {
		this.movieGenre = movieGenre;
	}

	@Override
	public void readFields(DataInput dataIp) throws IOException{
		this.movieId.readFields(dataIp);
		this.movieTitle.readFields(dataIp);
		this.movieGenre.readFields(dataIp);
		this.overallRating.readFields(dataIp);
	}

	@Override
	public void write(DataOutput dataOp) throws IOException {
		this.movieId.write(dataOp);
		this.movieTitle.write(dataOp);
		this.movieGenre.write(dataOp);
		this.overallRating.write(dataOp);
	}

	@Override
	public int compareTo(Object obj1) {
		return this.movieId.compareTo(((MovieWritable)obj1).movieId);
	}
}
