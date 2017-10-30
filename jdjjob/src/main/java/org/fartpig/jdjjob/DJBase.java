package org.fartpig.jdjjob;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DJBase {

	private static final Logger logger = LoggerFactory.getLogger("jdjjob");

	// error severity levels
	protected static final int CRITICAL = 4;
	protected static final int ERROR = 3;
	protected static final int WARN = 2;
	protected static final int INFO = 1;
	protected static final int DEBUG = 0;

	/**
	 * @var string
	 */
	public static String jobsTable = "jobs";

	/**
	 * @var int
	 */
	private static int retries = 3; // default retries

	// use either `configure` or `setConnection`, depending on if
	// you already have a PDO object you can re-use

	/**
	 * Configures DJJob with certain values for the database connection.
	 */
	@SuppressWarnings("unchecked")
	public static void configure(Object[] args) {
		int numArgs = args.length;

		switch (numArgs) {
		case 1: {
			if (args[0] instanceof Map) {
				configureWithOptions((Map<String, String>) args[0], null);
			} else {
				configureWithOptions(null, null);
			}
			break;
		}
		case 2: {
			if (args[0] instanceof Map) {
				configureWithOptions((Map<String, String>) args[0], (String) args[1]);
			} else {
				configureWithOptions((Map<String, String>) args[1], null);
			}
			break;
		}
		case 3: {
			configureWithOptions((Map<String, String>) args[1], (String) args[2]);
			break;
		}
		}
	}

	/**
	 * Configures DJJob with certain values for the database connection.
	 *
	 * @param Map<String,
	 *            String> options The options for the PDO connection.
	 * @param string
	 *            jobsTable Name of the jobs table.
	 *
	 * @throws DJException
	 *             Throws an exception with invalid parameters.
	 */
	protected static void configureWithOptions(Map<String, String> options, String jobsTable) {
		if (jobsTable == null || jobsTable.length() == 0) {
			jobsTable = "jobs";
		}

		if (options == null) {
			options = new HashMap<String, String>();
		}

		DJBase.jobsTable = jobsTable;

		// searches for retries
		if (options.containsKey("retries")) {
			DJBase.retries = (int) Integer.valueOf(options.get(retries));
		}
	}

	protected static void log(int severity, String mesg, Object... args) {
		log(String.format(mesg, args), severity);
	}

	/**
	 * Logs a message to the output.
	 *
	 * @param string
	 *            $mesg The message to log.
	 * @param int
	 *            $severity The log level necessary for this message to display.
	 */
	protected static void log(String mesg, int severity) {
		switch (severity) {
		case DJBase.CRITICAL:
			logger.error(mesg);
			break;
		case DJBase.ERROR:
			logger.error(mesg);
			break;
		case DJBase.WARN:
			logger.warn(mesg);
			break;
		case DJBase.INFO:
			logger.info(mesg);
			break;
		case DJBase.DEBUG:
			logger.debug(mesg);
			break;
		}
	}

}
