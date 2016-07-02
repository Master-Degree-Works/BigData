package cat.eps.movieRecommender.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class UserWritable implements WritableComparable<Object> {


	//User
		LongWritable userId;
		LongWritable userAge;
		Text userGenre;
		LongWritable userOccupation;
		Text userZip;
	
		LongWritable userOcurrences;

	public UserWritable(Text value) throws JSONException {
			JSONObject obj = new JSONObject(value.toString());
			this.userId=new LongWritable(obj.getLong("userId"));
			this.userAge=new LongWritable(obj.getLong("userAge"));
			this.userGenre=new Text(obj.getString("userGenre"));
			this.userOccupation = new LongWritable(obj.getLong("userOccupation"));
			this.userZip = new Text(obj.getString("userZip"));
	}

	public UserWritable() {
		this.userId=new LongWritable(0);
		this.userAge=new LongWritable(0);
		this.userGenre=new Text("");
		this.userOccupation = new LongWritable(0);
		this.userZip = new Text("");
	}

	public UserWritable(JSONObject obj) throws JSONException {	
		this.userId=new LongWritable(obj.getLong("userId"));
		this.userAge=new LongWritable(obj.getLong("userAge"));
		this.userGenre=new Text(obj.getString("userGenre"));
		this.userOccupation = new LongWritable(obj.getLong("userOccupation"));
		this.userZip = new Text(obj.getString("userZip"));
	}



	public UserWritable(String string, boolean addRatings) {
		try {
			JSONObject obj = new JSONObject(string);
			
			this.userId=new LongWritable(obj.getLong("userId"));
			this.userAge=new LongWritable(obj.getLong("userAge"));
			this.userGenre=new Text(obj.getString("userGenre"));
			this.userOccupation = new LongWritable(obj.getLong("userOccupation"));
			this.userZip = new Text(obj.getString("userZip"));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	
	public UserWritable(String string) throws JSONException {
		JSONObject obj = new JSONObject(string);
		this.userId=new LongWritable(obj.getLong("userId"));
		this.userAge=new LongWritable(obj.getLong("userAge"));
		this.userGenre=new Text(obj.getString("userGenre"));
		this.userOccupation = new LongWritable(obj.getLong("userOccupation"));
		this.userZip = new Text(obj.getString("userZip"));
	}

	public LongWritable getUserOcurrences() {
		return userOcurrences;
	}

	public void setUserOcurrences(LongWritable userOcurrences) {
		this.userOcurrences = userOcurrences;
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

	public String toStringSimple() {
		StringBuilder strBOut = new StringBuilder();
		strBOut.append("{");
		strBOut.append("\"userId\":"+this.getUserId()+",");
		strBOut.append("\"userAge\":"+ this.getUserAge().toString()+",");
		strBOut.append("\"userGenre\":\""+this.getUserGenre().toString()+"\",");
		strBOut.append("\"userOccupation\":"+ this.getUserOccupation().toString()+",");
		strBOut.append("\"userZip\":\""+ this.getUserZip().toString()+"\",");
		strBOut.append("\"userOcurrences\":"+ this.getUserOcurrences().toString());
		strBOut.append("}");
		
		return strBOut.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder strBOut = new StringBuilder();
		strBOut.append("{");
		strBOut.append("\"userId\":"+this.getUserId()+",");
		strBOut.append("\"userAge\":"+ this.getUserAge().toString()+",");
		strBOut.append("\"userGenre\":\""+this.getUserGenre().toString()+"\",");
		strBOut.append("\"userOccupation\":"+ this.getUserOccupation().toString()+",");
		strBOut.append("\"userZip\":\""+ this.getUserZip().toString()+"\",");
		strBOut.append("\"userOcurrences\":"+ this.getUserOcurrences().toString());
		strBOut.append("},");
		
		return strBOut.toString();
	}
	

	@Override
	public void readFields(DataInput dataIp) throws IOException{
		this.userId.readFields(dataIp);
		this.userAge.readFields(dataIp);
		this.userGenre.readFields(dataIp);
		this.userOccupation.readFields(dataIp);
		this.userZip.readFields(dataIp);
		this.userOcurrences.readFields(dataIp);
	}

	@Override
	public void write(DataOutput dataOp) throws IOException {
		this.userId.write(dataOp);
		this.userAge.write(dataOp);
		this.userGenre.write(dataOp);
		this.userOccupation.write(dataOp);
		this.userZip.write(dataOp);
		this.userOcurrences.write(dataOp);
	}

	@Override
	public int compareTo(Object obj1) {
		return this.userId.compareTo(((UserWritable)obj1).userId);
	}
}
