package cat.eps.movieRecommender.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class JsonWritable implements WritableComparable<Object> {

	LongWritable id;

	//User
	LongWritable userId;
	LongWritable userAge;
	Text userGenre;
	LongWritable userOccupation;
	Text userZip;

	//Movie
	LongWritable movieId;
	Text movieTitle;
	Text movieGenre;

	//Rating
	IntWritable rating;
	LongWritable timestamp;

	public JsonWritable() {
		this.id = new LongWritable();

		this.userId=new LongWritable();
		this.userAge=new LongWritable();
		this.userGenre = new Text();
		this.userOccupation = new LongWritable();
		this.userZip=new Text("");

		this.movieId=new LongWritable();
		this.movieTitle=new Text("");
		this.movieGenre=new Text("");

		this.rating=new IntWritable();
		this.timestamp=new LongWritable();
	}

	public JsonWritable(long id, JSONObject obj) {
		this.id = new LongWritable(id);

		try {
			this.userId=new LongWritable(obj.getLong("userId"));
			this.userAge=new LongWritable(obj.getLong("userAge"));
			this.userGenre = new Text(obj.getString("userGenre"));
			this.userOccupation = new LongWritable(obj.getLong("userOccupation"));
			this.userZip=new Text(obj.getString("userGenre"));

			this.movieId=new LongWritable(obj.getLong("movieId"));
			this.movieTitle=new Text(obj.getString("movieTitle"));
			this.movieGenre=new Text(obj.getString("movieGenre"));

			this.rating=new IntWritable(obj.getInt("rating"));
			this.timestamp=new LongWritable(obj.getLong("timestamp"));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}


	public LongWritable getId() {
		return id;
	}

	public void setId(LongWritable id) {
		this.id = id;
	}

	public LongWritable getUserId() {
		return userId;
	}

	public void setUserId(LongWritable userId) {
		this.userId = userId;
	}

	public LongWritable getUserAge() {
		return userAge;
	}

	public void setUserAge(LongWritable userAge) {
		this.userAge = userAge;
	}

	public Text getUserGenre() {
		return userGenre;
	}

	public void setUserGenre(Text userGenre) {
		this.userGenre = userGenre;
	}

	public LongWritable getUserOccupation() {
		return userOccupation;
	}

	public void setUserOccupation(LongWritable userOccupation) {
		this.userOccupation = userOccupation;
	}

	public Text getUserZip() {
		return userZip;
	}

	public void setUserZip(Text userZip) {
		this.userZip = userZip;
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

	public IntWritable getRating() {
		return rating;
	}

	public void setRating(IntWritable rating) {
		this.rating = rating;
	}


	public LongWritable getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(LongWritable timestamp) {
		this.timestamp = timestamp;
	}



	@Override
	public void readFields(DataInput dataIp) throws IOException{

		this.id.readFields(dataIp);

		this.userId.readFields(dataIp);
		this.userAge.readFields(dataIp);
		this.userGenre.readFields(dataIp);
		this.userOccupation.readFields(dataIp);
		this.userZip.readFields(dataIp);

		this.movieId.readFields(dataIp);
		this.movieTitle.readFields(dataIp);
		this.movieGenre.readFields(dataIp);

		this.rating.readFields(dataIp);
		this.timestamp.readFields(dataIp);
	}

	@Override
	public void write(DataOutput dataOp) throws IOException {

		this.id.write(dataOp);

		this.userId.write(dataOp);
		this.userAge.write(dataOp);
		this.userGenre.write(dataOp);
		this.userOccupation.write(dataOp);
		this.userZip.write(dataOp);

		this.movieId.write(dataOp);
		this.movieTitle.write(dataOp);
		this.movieGenre.write(dataOp);

		this.rating.write(dataOp);
		this.timestamp.write(dataOp);

	}

	@Override
	public int compareTo(Object obj1) {
		return this.id.compareTo(((JsonWritable)obj1).id);
	}
}
