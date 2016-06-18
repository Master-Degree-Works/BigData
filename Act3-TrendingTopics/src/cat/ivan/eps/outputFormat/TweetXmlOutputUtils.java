package cat.ivan.eps.outputFormat;

public class TweetXmlOutputUtils {

	public static String getTweetId(String[] contents) {
		return contents[0]; 
	}

	public static String getTweetMessage(String[] contents) {
		return contents[1]; 
	}

	public static String getTweetLanguage(String[] contents) {
		return contents[2];
	}
	
	public static String getTweetSentiment(String[] contents) {
		return contents[3];
	}
}
