package org.fartpig.jdjjob;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.fartpig.jdjjob.dao.DJJobDao;

import junit.framework.TestCase;

public class TestCustomTableName extends TestCase {

	public static class HelloWorldJob implements DJJobHandlerInterface {

		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public HelloWorldJob(String name) {
			this.name = name;
		}

		public HelloWorldJob() {

		}

		public void perform() throws Exception {
			System.out.println(String.format("Hello %s!\n", this.name));
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public void onDjjobRetryError(String error) {
			System.out.println(String.format("error in HelloWorldJob: %s!\n", error));
		}
	}

	public static class FailingJob implements DJJobHandlerInterface {

		public FailingJob() {

		}

		public void perform() throws Exception {
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			throw new Exception("Uh oh");
		}

		public void onDjjobRetryError(String error) {
			System.out.println(String.format("error in FailingJob: %s!\n", error));
		}
	}

	public void testCustTable() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put("dbname", "djjob");
		DJJob.configure(new Object[] { config, "my_jobs" });

		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ");
		sb.append(DJBase.jobsTable);
		dao.execute(sb.toString(), Collections.<Object>emptyList());

		Map<String, Object> status = DJJob.status("default");

		assertEquals(((Long) status.get("outstanding")).longValue(), 0L);
		assertEquals(((Long) status.get("locked")).longValue(), 0L);
		assertEquals(((Long) status.get("failed")).longValue(), 0L);
		assertEquals(((Long) status.get("total")).longValue(), 0L);

		System.out.println("=====================\nStarting run of DJJob\n=====================\n\n");

		DJJob.enqueue(new HelloWorldJob("delayed_job"), "default", null);
		DJJob.bulkEnqueue(
				Arrays.<DJJobHandlerInterface>asList(new HelloWorldJob("shopify"), new HelloWorldJob("github")),
				"default", null);

		DJJob.enqueue(new FailingJob(), "default", null);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put("count", 5);
		options.put("max_attempts", 2);
		options.put("sleep", 10);

		String workPrefix = "fartpig:";
		DJWorker worker = new DJWorker(options, workPrefix);
		worker.start();
		System.out.println("\n============\nRun complete\n============\n\n");

		status = DJJob.status("default");

		assertEquals(((Long) status.get("outstanding")).longValue(), 0L);
		assertEquals(((Long) status.get("locked")).longValue(), 0L);
		assertEquals(((Long) status.get("failed")).longValue(), 1L);
		assertEquals(((Long) status.get("total")).longValue(), 1L);

	}
}
