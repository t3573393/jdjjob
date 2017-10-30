package org.fartpig.jdjjob;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fartpig.jdjjob.dao.DJJobDao;

/**
 * The worker class that can empty a queue.
 */
public class DJWorker extends DJBase {

	// This is a singleton-ish thing. It wouldn't really make sense to
	// instantiate more than one in a single request (or commandline task)

	private String queue;
	private int count;
	private int sleep;
	private int maxAttempts;
	private boolean failOnOutput = false;
	private String hostName;

	private String name;

	/**
	 * DJWorker constructor.
	 *
	 * The following options are available: `queue`: The queue to work on.
	 * Default: 'default' `count`: How many jobs to execute before exiting. Use
	 * '0' for no-limit. Default: '0' `sleep`: How long to sleep if no jobs are
	 * found. Default: '5' `max_attempts`: How many times to try a job before
	 * bailing out. Default: '5' `fail_on_output`: Whether to fail on output.
	 * Default: 'false'
	 *
	 * @param Map<String,Object>
	 *            options The settings for this worker.
	 * @param String
	 *            workPrefix the worker prefixName
	 */
	public DJWorker(Map<String, Object> options, String workPrefix) {
		Map<String, Object> originalMap = new HashMap<String, Object>();
		originalMap.put("queue", "default");
		originalMap.put("count", 0);
		originalMap.put("sleep", 5);
		originalMap.put("max_attempts", 5);
		originalMap.put("fail_on_output", false);

		options = Utils.mergeMaps(originalMap, options);

		this.queue = (String) options.get("queue");
		this.count = (Integer) options.get("count");
		this.sleep = ((Integer) options.get("sleep")) * 1000;
		this.maxAttempts = (Integer) options.get("max_attempts");
		this.failOnOutput = (Boolean) options.get("fail_on_output");

		String hostname = "Unknown";

		try {
			InetAddress addr;
			addr = InetAddress.getLocalHost();
			hostname = addr.getHostName();
		} catch (UnknownHostException ex) {
			System.out.println("Hostname can not be resolved");
		}
		this.hostName = hostname;

		this.name = String.format("%s@%s", workPrefix, this.hostName);

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				DJWorker.this.handleSignal();
			}

		});
	}

	/**
	 * Handles a signal from the operating system.
	 *
	 */
	public void handleSignal() {
		log("[WORKER] Received received signal ... Shutting down", DJBase.INFO);
		this.releaseLocks();
	}

	/**
	 * Releases all locks this worker has on the jobs table.
	 */
	public void releaseLocks() {

		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET locked_at = NULL, locked_by = NULL ");
		sb.append(" WHERE locked_by = ? ");
		List<Object> args = new ArrayList<Object>();
		args.add(this.name);
		dao.execute(sb.toString(), args);
	}

	/**
	 * Returns a new job ordered by most recent first why this? run newest
	 * first, some jobs get left behind run oldest first, all jobs get left
	 * behind
	 *
	 * @return \DJJob|false A job if one was successfully locked. Otherwise
	 *         false.
	 */
	public DJJob getNewJob() {
		// we can grab a locked job if we own the lock
		DJJobDao dao = new DJJobDao();

		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT id FROM ");
		sb.append(DJBase.jobsTable);
		sb.append(" WHERE  queue = ? ");
		sb.append(" AND    (run_at IS NULL OR NOW() >= run_at) ");
		sb.append(" AND    (locked_at IS NULL OR locked_by = ?) ");
		sb.append(" AND    failed_at IS NULL");
		sb.append(" AND    attempts < ? ");
		sb.append(" ORDER BY created_at DESC ");
		sb.append(" LIMIT  10 ");
		List<Object> args = new ArrayList<Object>();
		args.add(this.queue);
		args.add(this.name);
		args.add(this.maxAttempts);

		List<Object[]> rs = dao.executeQuery(sb.toString(), args);

		// randomly order the 10 to prevent lock contention among workers
		Collections.shuffle(rs);

		Map<String, Object> options = new HashMap<String, Object>();
		options.put("max_attempts", this.maxAttempts);
		options.put("fail_on_output", this.failOnOutput);

		for (Object[] obj : rs) {
			DJJob job = new DJJob(this.name, (Long) obj[0], options);
			if (job.acquireLock()) {
				return job;
			}
		}

		return null;
	}

	/**
	 * Starts the worker process.
	 */
	public void start() {
		log(DJBase.INFO, "[JOB] Starting worker %s on queue::%s", this.name, this.queue);

		int count = 0;
		int jobCount = 0;
		try {
			while (this.count == 0 || count < this.count) {

				count += 1;
				DJJob job = this.getNewJob();

				if (job == null) {
					log(DJBase.DEBUG, "[JOB] Failed to get a job, queue::%s may be empty", this.queue);
					Thread.sleep(this.sleep);
					continue;
				}

				jobCount += 1;
				job.run();
			}
		} catch (Exception e) {
			log(DJBase.ERROR, "[JOB] unhandled exception::\"%s\"", e.getMessage());
			e.printStackTrace();
		}

		log(DJBase.INFO, "[JOB] worker shutting down after running %d jobs, over %d polling iterations", jobCount,
				count);
	}
}
