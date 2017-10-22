package org.fartpig.jdjjob;

/**
 * Job handler interface which can be safely enqueued.
 */
public interface DJJobHandlerInterface {
	/**
	 * Method that will perform the job when retrieved from the jobs table.
	 */
	public void perform();
}
