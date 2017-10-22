package org.fartpig.jdjjob;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

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
	 * The following options are available: `queue`: The queue to work on. Default:
	 * 'default' `count`: How many jobs to execute before exiting. Use '0' for
	 * no-limit. Default: '0' `sleep`: How long to sleep if no jobs are found.
	 * Default: '5' `max_attempts`: How many times to try a job before bailing out.
	 * Default: '5' `fail_on_output`: Whether to fail on output. Default: 'false'
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
		this.sleep = (Integer) options.get("sleep");
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

		if (function_exists("pcntl_signal")) {
			pcntl_signal(SIGTERM, array($this, "handleSignal"));
			pcntl_signal(SIGINT, array($this, "handleSignal"));
		}
	}

	/**
     * Handles a signal from the operating system.
     *
     * @param string $signo The signal received from the OS.
     */
    public function handleSignal($signo) {
        $signals = array(
            SIGTERM => "SIGTERM",
            SIGINT  => "SIGINT"
        );
        $signal = $signals[$signo];

        $this->log("[WORKER] Received received {$signal}... Shutting down", self::INFO);
        $this->releaseLocks();
        die(0);
    }

	/**
     * Releases all locks this worker has on the jobs table.
     */
    public function releaseLocks() {
        $this->runUpdate("
            UPDATE " . self::$jobsTable . "
            SET locked_at = NULL, locked_by = NULL
            WHERE locked_by = ?",
            array($this->name)
        );
    }

	/**
     * Returns a new job ordered by most recent first
     * why this?
     *     run newest first, some jobs get left behind
     *     run oldest first, all jobs get left behind
     *
     * @return \DJJob|false A job if one was successfully locked. Otherwise false.
     */
    public function getNewJob() {
        # we can grab a locked job if we own the lock
        $rs = $this->runQuery("
            SELECT id
            FROM   " . self::$jobsTable . "
            WHERE  queue = ?
            AND    (run_at IS NULL OR NOW() >= run_at)
            AND    (locked_at IS NULL OR locked_by = ?)
            AND    failed_at IS NULL
            AND    attempts < ?
            ORDER BY created_at DESC
            LIMIT  10
        ", array($this->queue, $this->name, $this->max_attempts));

        // randomly order the 10 to prevent lock contention among workers
        shuffle($rs);

        foreach ($rs as $r) {
            $job = new DJJob($this->name, $r["id"], array(
                "max_attempts" => $this->max_attempts,
                "fail_on_output" => $this->fail_on_output
            ));
            if ($job->acquireLock()) return $job;
        }

        return false;
    }

	/**
     * Starts the worker process.
     */
    public function start() {
        $this->log("[JOB] Starting worker {$this->name} on queue::{$this->queue}", self::INFO);

        $count = 0;
        $job_count = 0;
        try {
            while ($this->count == 0 || $count < $this->count) {
                if (function_exists("pcntl_signal_dispatch")) pcntl_signal_dispatch();

                $count += 1;
                $job = $this->getNewJob($this->queue);

                if (!$job) {
                    $this->log("[JOB] Failed to get a job, queue::{$this->queue} may be empty", self::DEBUG);
                    sleep($this->sleep);
                    continue;
                }

                $job_count += 1;
                $job->run();
            }
        } catch (Exception $e) {
            $this->log("[JOB] unhandled exception::\"{$e->getMessage()}\"", self::ERROR);
        }

        $this->log("[JOB] worker shutting down after running {$job_count} jobs, over {$count} polling iterations", self::INFO);
    }
}
