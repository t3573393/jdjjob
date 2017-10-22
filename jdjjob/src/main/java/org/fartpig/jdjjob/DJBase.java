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
	 * @var int
	 */
	private static int logLevel = DEBUG;

	/**
	 * @var string
	 */
	protected static String jobsTable = "";

	/**
	 * @var string
	 */
	private static String dsn = "";

	/**
	 * @var string
	 */
	private static String user = "";

	/**
	 * @var string
	 */
	private static String password = "";

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
				configureWithDsnAndOptions((String) args[0], null, null);
			}
			break;
		}
		case 2: {
			if (args[0] instanceof Map) {
				configureWithOptions((Map<String, String>) args[0], (String) args[1]);
			} else {
				configureWithDsnAndOptions((String) args[0], (Map<String, String>) args[1], null);
			}
			break;
		}
		case 3: {
			configureWithDsnAndOptions((String) args[0], (Map<String, String>) args[1], (String) args[2]);
			break;
		}
		}
	}

	/**
	 * Configures DJJob with certain values for the database connection.
	 *
	 * @param $dsn
	 *            The PDO connection string.
	 * @param array
	 *            $options The options for the PDO connection.
	 * @param string
	 *            $jobsTable Name of the jobs table.
	 *
	 * @throws DJException
	 *             Throws an exception with invalid parameters.
	 */
	protected static void configureWithDsnAndOptions(String dsn, Map<String, String> options, String jobsTable) {
		if (jobsTable == null || jobsTable.length() == 0) {
			jobsTable = "jobs";
		}

		if (options == null) {
			options = new HashMap<String, String>();
		}

		if (!options.containsKey("mysql_user")) {
			throw new DJException("Please provide the database user in configure options array.");
		}
		if (!options.containsKey("mysql_pass")) {
			throw new DJException("Please provide the database password in configure options array.");
		}

		DJBase.dsn = dsn;
		DJBase.jobsTable = jobsTable;

		DJBase.user = options.get("mysql_user");
		DJBase.password = options.get("mysql_pass");

		// searches for retries
		if (options.containsKey("retries")) {
			DJBase.retries = (int) Integer.valueOf(options.get(retries));
		}
	}

	/**
	 * @param array
	 *            $options
	 * @param string
	 *            $jobsTable
	 *
	 * @throws DJException
	 *             Throws an exception with invalid parameters.
	 */
	protected static void configureWithOptions(Map<String, String> options, String jobsTable) {

		if (jobsTable == null || jobsTable.length() == 0) {
			jobsTable = "jobs";
		}

		if (!options.containsKey("driver")) {
			throw new DJException("Please provide the database driver used in configure options array.");
		}
		if (!options.containsKey("user")) {
			throw new DJException("Please provide the database user in configure options array.");
		}
		if (!options.containsKey("password")) {
			throw new DJException("Please provide the database password in configure options array.");
		}

		DJBase.user = options.get("user");
		DJBase.password = options.get("password");
		DJBase.jobsTable = jobsTable;

		DJBase.dsn = options.get("driver") + ":";
		for (Map.Entry<String, String> enty : options.entrySet()) {
			// skips options already used
			if (enty.getKey().equals("driver") || enty.getKey().equals("user") || enty.getKey().equals("password")) {
				continue;
			}

			// searches for retries
			if (enty.getKey().equals("retries")) {
				DJBase.retries = (int) Integer.valueOf(enty.getValue());
				continue;
			}

			DJBase.dsn += enty.getKey() + "=" + enty.getValue() + ";";
		}

	}

	/**
	 * @param int
	 *            $const The log level to set.
	 */
	public static void setLogLevel(int logLevel) {
		DJBase.logLevel = logLevel;
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
