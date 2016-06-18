package cat.ivan.eps.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TweetWritable implements WritableComparable<Object> {
	
	private Text hashtag;
	private Text message;
	private DoubleWritable sentiment;

	private Text tweetId;
	private Text idioma;
	private Text usuari;

	public TweetWritable() {
		this.hashtag=new Text("");
		this.message=new Text("");
		this.sentiment = new DoubleWritable(0);
		this.tweetId = new Text("");
		this.idioma=new Text("");
		this.usuari=new Text("");
	}

	public TweetWritable(String hashtag, String message, Double sentiment) {
		super();
		this.hashtag = new Text(hashtag);
		this.message = new Text(message);
		this.sentiment = new DoubleWritable(sentiment);
	}

	public TweetWritable( String tweetId,String message, String idioma, String usuari) {
		super();
		this.message = new Text(message);
		this.tweetId = new Text(tweetId);
		this.idioma = new Text(idioma);
		this.usuari =new Text(usuari);
		this.sentiment = new DoubleWritable(0l);
		this.hashtag = new Text("");
	}



	public TweetWritable(String tweetId, String message, String idioma) {
		this.message = new Text(message);
		this.tweetId = new Text(tweetId);
		this.idioma = new Text(idioma);
		this.sentiment = new DoubleWritable(0l);
		this.hashtag = new Text("");
	}




	public Text getHashtag() {
		return hashtag;
	}

	public void setHashtag(Text hashtag) {
		this.hashtag = hashtag;
	}

	public Text getMessage() {
		return message;
	}

	public void setMessage(Text message) {
		this.message = message;
	}

	public DoubleWritable getSentiment() {
		return sentiment;
	}

	public void setSentiment(DoubleWritable sentiment) {
		this.sentiment = sentiment;
	}

	public Text getTweetId() {
		return tweetId;
	}

	public void setTweetId(Text tweetId) {
		this.tweetId = tweetId;
	}

	public Text getIdioma() {
		return idioma;
	}

	public void setIdioma(Text idioma) {
		this.idioma = idioma;
	}

	public Text getUsuari() {
		return usuari;
	}

	public void setUsuari(Text usuari) {
		this.usuari = usuari;
	}

	@Override
	public void readFields(DataInput dataIp) throws IOException {
		tweetId.readFields(dataIp);
		message.readFields(dataIp);
		idioma.readFields(dataIp);
		hashtag.readFields(dataIp);
		sentiment.readFields(dataIp);
	}

	@Override
	public void write(DataOutput dataOp) throws IOException {
		tweetId.write(dataOp);
		message.write(dataOp);
		idioma.write(dataOp);
		hashtag.write(dataOp);
		sentiment.write(dataOp);
	}

	@Override
	public int compareTo(Object obj1) {
		return tweetId.compareTo(((TweetWritable)obj1).tweetId);
	}
}
