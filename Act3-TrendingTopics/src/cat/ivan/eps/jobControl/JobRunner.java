package cat.ivan.eps.jobControl;

import org.apache.hadoop.mapred.jobcontrol.JobControl;

public class JobRunner implements Runnable {
	private JobControl control;
	
	public JobRunner(JobControl _control) {
		this.control = _control;
	}
	public void run() {
		this.control.run();
	}
}