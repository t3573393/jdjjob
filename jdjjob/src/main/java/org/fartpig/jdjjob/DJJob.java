package org.fartpig.jdjjob;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fartpig.jdjjob.dao.DJJobDao;

/**
 * Represents a job that needs to be executed.
 */
public class DJJob extends DJBase {

	private String workerName;
	private long jobId;

	private int maxAttempts;

	/**
	 * Constructs the Job
	 *
	 * Possible options: `max_attempts`: The amount of attempts before bailing
	 * out. Default: '5'
	 *
	 * @param string
	 *            workerName Name of the worker that created this job.
	 * @param long
	 *            jobId ID of this job.
	 * @param array
	 *            options The options.
	 */
	public DJJob(String workerName, long jobId, Map<String, Object> options) {
		Map<String, Object> originalMap = new HashMap<String, Object>();
		originalMap.put("max_attempts", 5);

		options = Utils.mergeMaps(originalMap, options);

		this.workerName = workerName;
		this.jobId = jobId;
		this.maxAttempts = (Integer) options.get("max_attempts");
	}

	/**
	 * Runs this job.
	 *
	 * First retrieves the handler fro the database. Then perform the job.
	 *
	 * @return boolean Whether or not the job succeeded.
	 */
	public boolean run() {
		// pull the handler from the db
		DJJobHandlerInterface handler = this.getHandler();
		if (handler == null) {
			String msg = String.format("[JOB] bad handler for job::%d", this.jobId);
			this.finishWithError(msg, handler);
			return false;
		}

		// run the handler
		try {

			handler.perform();

			// cleanup
			this.finish();
			return true;

		} catch (DJRetryException e) {

			// attempts hasn't been incremented yet.
			int attempts = this.getAttempts() + 1;

			String msg = String.format("Caught DJRetryException \"%s\" on attempt %d/%d.", e.getMessage(), attempts,
					this.maxAttempts);

			if (attempts == this.maxAttempts) {
				msg = String.format("[JOB] job::%d %s Giving up.", this.jobId, msg);
				this.finishWithError(msg, handler);
			} else {
				log(String.format("[JOB] job::%d %s Try again in %d seconds.", this.jobId, msg, e.getDelay()),
						DJBase.WARN);
				this.retryLater(e.getDelay());
			}
			return false;

		} catch (Exception e) {
			this.finishWithError(e.getMessage(), handler);
			return false;

		}
	}

	/**
	 * Acquires lock on this job.
	 *
	 * @return boolean Whether or not acquiring the lock succeeded.
	 */
	public boolean acquireLock() {
		log(String.format("[JOB] attempting to acquire lock for job::%d on %s", this.jobId, this.workerName),
				DJBase.INFO);
		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET locked_at=NOW(), locked_by=? ");
		sb.append(" WHERE id=? AND (locked_at IS NULL OR locked_by=?) AND failed_at IS NULL");
		List<Object> args = new ArrayList<Object>();
		args.add(this.workerName);
		args.add(this.jobId);
		args.add(this.workerName);
		boolean lock = dao.execute(sb.toString(), args);
		if (!lock) {
			log(DJBase.INFO, "[JOB] failed to acquire lock for job::%d", this.jobId);
			return false;
		}

		return true;
	}

	/**
	 * Releases the lock on this job.
	 */
	public void releaseLock() {
		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET locked_at = NULL, locked_by = NULL ");
		sb.append(" WHERE id = ?");
		List<Object> args = new ArrayList<Object>();
		args.add(this.jobId);
		dao.execute(sb.toString(), args);
	}

	/**
	 * Finishes this job. Will delete it from the jobs table.
	 */
	public void finish() {
		DJJobDao dao = new DJJobDao();
		Job job = new Job();
		job.setId(this.jobId);
		dao.delete(job);
		log(DJBase.INFO, "[JOB] completed job::%d", this.jobId);
	}

	/**
	 * Finishes this job, but with an error. Keeps the job in the jobs table.
	 *
	 * @param string
	 *            error The error message to write to the job.
	 * @param null|object
	 *            DJJobHandlerInterface The handler that ran this job.
	 */
	public void finishWithError(String error, DJJobHandlerInterface handler) {
		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET attempts = attempts + 1, ");
		sb.append(" failed_at = IF(attempts >= ?, NOW(), NULL), ");
		sb.append(" error = IF(attempts >= ?, ?, NULL) ");
		sb.append(" WHERE id = ?");
		List<Object> args = new ArrayList<Object>();
		args.add(this.maxAttempts);
		args.add(this.maxAttempts);
		args.add(error);
		args.add(this.jobId);
		dao.execute(sb.toString(), args);

		log(error, DJBase.ERROR);
		log(DJBase.ERROR, "[JOB] failure in job::%d", this.jobId);
		this.releaseLock();

		if (handler != null && (this.getAttempts() == this.maxAttempts)) {
			handler.onDjjobRetryError(error);
		}
	}

	/**
	 * Saves a retry date to this job.
	 *
	 * @param int
	 *            delay The amount of seconds to delay this job.
	 */
	public void retryLater(long delay) {
		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET run_at = DATE_ADD(NOW(), INTERVAL ? SECOND), ");
		sb.append(" attempts = attempts + 1 ");
		sb.append(" WHERE id = ?");
		List<Object> args = new ArrayList<Object>();
		args.add(delay);
		args.add(this.jobId);
		dao.execute(sb.toString(), args);
		this.releaseLock();
	}

	/**
	 * Returns the handler for this job.
	 *
	 * @return bool|object The handler object for this job. Or false if it
	 *         failed.
	 * @throws ClassNotFoundException
	 */
	public DJJobHandlerInterface getHandler() {
		DJJobDao dao = new DJJobDao();
		Job job = dao.loadByJobId(this.jobId);
		if (job != null) {
			String handlerStr = job.getHandler();
			// format: classname:{jsonstr}
			if (handlerStr != null && handlerStr.length() > 0) {
				int clazzIndex = handlerStr.indexOf(":");
				String clazzStr = handlerStr.substring(0, clazzIndex);
				Object obj;
				try {
					obj = Utils.deserializationObj(handlerStr.substring(clazzIndex + 1), Class.forName(clazzStr));
					if (DJJobHandlerInterface.class.isInstance(obj)) {
						return (DJJobHandlerInterface) obj;
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			}
		}
		return null;
	}

	/**
	 * Returns the amount of attempts left for this job.
	 *
	 * @return bool The amount of attempts left.
	 */
	public int getAttempts() {
		DJJobDao dao = new DJJobDao();
		Job job = dao.loadByJobId(this.jobId);
		if (job != null) {
			return job.getAttempts();
		}
		return 0;
	}

	private static String convertObjectToHandlerStr(DJJobHandlerInterface handler) {
		return handler.getClass().getName() + ":" + Utils.serializationObj(handler);
	}

	/**
	 * Enqueues a job to the database.
	 *
	 * @param DJJobHandlerInterface
	 *            handler The handler that can execute this job.
	 * @param string
	 *            queue The queue to enqueue this job to. All queues are saved
	 *            in the same table.
	 * @param string
	 *            runAt A valid mysql DATETIME string at which to run the jobs.
	 *
	 * @return bool Returns insert result
	 *
	 */
	public static boolean enqueue(DJJobHandlerInterface handler, String queue, Date runAt) {
		if (queue == null) {
			queue = "default";
		}

		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" INSERT INTO  ");
		sb.append(DJBase.jobsTable);
		sb.append("  (handler, queue, run_at, created_at) VALUES(?, ?, ?, NOW()) ");
		List<Object> args = new ArrayList<Object>();
		args.add(convertObjectToHandlerStr(handler));
		args.add(queue);
		args.add(runAt);
		boolean result = dao.execute(sb.toString(), args);
		if (!result) {
			log(DJBase.ERROR, "[JOB] failed to enqueue new job");
			return false;
		}

		return true;
	}

	/**
	 * Bulk enqueues a lot of jobs to the database.
	 *
	 * @param List<DJJobHandlerInterface>
	 *            handlers An array of handlers to enqueue.
	 * @param string
	 *            queue The queue to enqueue the handlers to.
	 * @param Date
	 *            run_at A valid mysql DATETIME string at which to run the jobs.
	 *
	 * @return bool
	 */
	public static boolean bulkEnqueue(List<DJJobHandlerInterface> handlers, String queue, Date runAt) {
		if (queue == null) {
			queue = "default";
		}

		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" INSERT INTO  ");
		sb.append(DJBase.jobsTable);
		sb.append("  (handler, queue, run_at, created_at) VALUES ");
		for (int i = 0; i < handlers.size(); i++) {
			if (i != 0) {
				sb.append(" , ");
			}
			sb.append(" (?, ?, ?, NOW())");
		}

		List<Object> args = new ArrayList<Object>();
		for (DJJobHandlerInterface handler : handlers) {
			args.add(convertObjectToHandlerStr(handler));
			args.add(queue);
			args.add(runAt);
		}

		boolean result = dao.execute(sb.toString(), args);
		if (!result) {
			log("[JOB] failed to enqueue new jobs", DJBase.ERROR);
			return false;
		}

		return true;
	}

	/**
	 * Returns the general status of the jobs table.
	 *
	 * @param string
	 *            queue The queue of which to see the status for.
	 *
	 * @return array Information about the status.
	 */
	public static Map<String, Object> status(String queue) {

		if (queue == null) {
			queue = "default";
		}

		DJJobDao dao = new DJJobDao();
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT COUNT(*) as total, COUNT(failed_at) as failed, COUNT(locked_at) as locked FROM ");
		sb.append(DJBase.jobsTable);
		sb.append(" WHERE queue = ? ");

		List<Object> args = new ArrayList<Object>();
		args.add(queue);

		List<Object[]> objList = dao.executeQuery(sb.toString(), args);

		long total = 0, failed = 0, locked = 0, outstanding = 0;

		if (objList.size() > 0) {
			Object[] objs = objList.get(0);

			total = (Long) objs[0];
			failed = (Long) objs[1];
			locked = (Long) objs[2];
			outstanding = total - locked - failed;
		}

		Map<String, Object> result = new HashMap<String, Object>();
		result.put("outstanding", outstanding);
		result.put("locked", locked);
		result.put("failed", failed);
		result.put("total", total);

		return result;
	}
}
