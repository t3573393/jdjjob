package org.fartpig.jdjjob;

/**
 * Exception thrown when the job should be retried after a specific period.
 */
public class DJRetryException extends DJException {

	public DJRetryException(String string) {
		super(string);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -5445375484341819202L;
	private long delaySeconds = 7200;

	public void setDelay(long delay) {
		this.delaySeconds = delay;
	}

	public long getDelay() {
		return this.delaySeconds;
	}
}
