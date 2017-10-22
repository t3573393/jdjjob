package org.fartpig.jdjjob;

/**
 * Represents a job that needs to be executed.
 */
public class DJJob extends DJBase {
	/**
     * Constructs the Job
     *
     * Possible options:
     * `max_attempts`: The amount of attempts before bailing out. Default: '5'
     * `fail_on_output`: Whether the job fails if there is output in the handler. Default: 'false'
     *
     * @param string $worker_name Name of the worker that created this job.
     * @param int $job_id ID of this job.
     * @param array $options The options.
     */
    public function __construct($worker_name, $job_id, $options = array()) {
        $options = array_merge(array(
            "max_attempts" => 5,
            "fail_on_output" => false
        ), $options);
        $this->worker_name = $worker_name;
        $this->job_id = $job_id;
        $this->max_attempts = $options["max_attempts"];
        $this->fail_on_output = $options["fail_on_output"];
    }

	/**
     * Runs this job.
     *
     * First retrieves the handler fro the database. Then perform the job.
     *
     * @return bool Whether or not the job succeeded.
     */
    public function run() {
        # pull the handler from the db
        $handler = $this->getHandler();
        if (!is_object($handler)) {
            $msg = "[JOB] bad handler for job::{$this->job_id}";
            $this->finishWithError($msg);
            return false;
        }

        # run the handler
        try {

            if ($this->fail_on_output) {
                ob_start();                
            }

            $handler->perform();

            if ($this->fail_on_output) {
                $output = ob_get_contents();
                ob_end_clean();

                if (!empty($output)) {
                    throw new Exception("Job produced unexpected output: $output");
                }
            }

            # cleanup
            $this->finish();
            return true;

        } catch (DJRetryException $e) {
            if ($this->fail_on_output) {
                ob_end_flush();
            }
            
            # attempts hasn't been incremented yet.
            $attempts = $this->getAttempts()+1;

            $msg = "Caught DJRetryException \"{$e->getMessage()}\" on attempt $attempts/{$this->max_attempts}.";

            if($attempts == $this->max_attempts) {
                $msg = "[JOB] job::{$this->job_id} $msg Giving up.";
                $this->finishWithError($msg, $handler);
            } else {
                $this->log("[JOB] job::{$this->job_id} $msg Try again in {$e->getDelay()} seconds.", self::WARN);
                $this->retryLater($e->getDelay());
            }
            return false;

        } catch (Exception $e) {
            if ($this->fail_on_output) {
                ob_end_flush();
            }

            $this->finishWithError($e->getMessage(), $handler);
            return false;

        }
    }

	/**
	 * Acquires lock on this job.
	 *
	 * @return bool Whether or not acquiring the lock succeeded.
	 */
	public function acquireLock() {
		$this->log("[JOB] attempting to acquire lock for job::{$this->job_id} on {$this->worker_name}",self::INFO);

		$lock=$this->runUpdate("UPDATE" . self::$jobsTable . "SET locked_at=NOW(),locked_by=?WHERE id=?AND(locked_at IS NULL OR locked_by=?)AND failed_at IS NULL", array($this->worker_name, $this->job_id, $this->worker_name));

		if(!$lock){$this->log("[JOB] failed to acquire lock for job::{$this->job_id}",self::INFO);return false;}

		return true;
	}

	/**
     * Releases the lock on this job.
     */
    public function releaseLock() {
        $this->runUpdate("
            UPDATE " . self::$jobsTable . "
            SET locked_at = NULL, locked_by = NULL
            WHERE id = ?",
            array($this->job_id)
        );
    }

	/**
	 * Finishes this job. Will delete it from the jobs table.
	 */
	public function finish() {
		$this->runUpdate("DELETE FROM ".self::$jobsTable." WHERE id = ?",array($this->job_id));$this->log("[JOB] completed job::{$this->job_id}",self::INFO);
	}

	/**
     * Finishes this job, but with an error. Keeps the job in the jobs table.
     *
     * @param string $error The error message to write to the job.
     * @param null|object $handler The handler that ran this job.
     */
    public function finishWithError($error, $handler = null) {
        $this->runUpdate("
            UPDATE " . self::$jobsTable . "
            SET attempts = attempts + 1,
                failed_at = IF(attempts >= ?, NOW(), NULL),
                error = IF(attempts >= ?, ?, NULL)
            WHERE id = ?",
            array(
                $this->max_attempts,
                $this->max_attempts,
                $error,
                $this->job_id
            )
        );
        $this->log($error, self::ERROR);
        $this->log("[JOB] failure in job::{$this->job_id}", self::ERROR);
        $this->releaseLock();

        if ($handler && ($this->getAttempts() == $this->max_attempts) && method_exists($handler, '_onDjjobRetryError')) {
          $handler->_onDjjobRetryError($error);
        }
    }

	/**
     * Saves a retry date to this job.
     *
     * @param int $delay The amount of seconds to delay this job.
     */
    public function retryLater($delay) {
        $this->runUpdate("
            UPDATE " . self::$jobsTable . "
            SET run_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                attempts = attempts + 1
            WHERE id = ?",
            array(
              $delay,
              $this->job_id
            )
        );
        $this->releaseLock();
    }

	/**
	 * Returns the handler for this job.
	 *
	 * @return bool|object The handler object for this job. Or false if it failed.
	 */
	public function getHandler() {
		$rs=$this->runQuery("SELECT handler FROM ".self::$jobsTable." WHERE id = ?",array($this->job_id));foreach($rs as $r)return unserialize($r["handler"]);return false;
	}

	/**
	 * Returns the amount of attempts left for this job.
	 *
	 * @return bool The amount of attempts left.
	 */
	public function getAttempts() {
		$rs=$this->runQuery("SELECT attempts FROM ".self::$jobsTable." WHERE id = ?",array($this->job_id));foreach($rs as $r)return $r["attempts"];return false;
	}

	/**
     * Enqueues a job to the database.
     *
     * @param object $handler The handler that can execute this job.
     * @param string $queue The queue to enqueue this job to. All queues are saved in the same table.
     * @param string $run_at A valid mysql DATETIME string at which to run the jobs.
     *
     * @return bool|string Returns the last inserted ID or false if enqueuing failed.
     */
    public static function enqueue($handler, $queue = "default", $run_at = null) {
        $affected = self::runUpdate(
            "INSERT INTO " . self::$jobsTable . " (handler, queue, run_at, created_at) VALUES(?, ?, ?, NOW())",
            array(serialize($handler), (string) $queue, $run_at)
        );

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new job", self::ERROR);
            return false;
        }

        return self::getConnection()->lastInsertId(); // return the job ID, for manipulation later
    }

	/**
     * Bulk enqueues a lot of jobs to the database.
     *
     * @param object[] $handlers An array of handlers to enqueue.
     * @param string $queue The queue to enqueue the handlers to.
     * @param string $run_at A valid mysql DATETIME string at which to run the jobs.
     *
     * @return bool
     */
    public static function bulkEnqueue($handlers, $queue = "default", $run_at = null) {
        $sql = "INSERT INTO " . self::$jobsTable . " (handler, queue, run_at, created_at) VALUES";
        $sql .= implode(",", array_fill(0, count($handlers), "(?, ?, ?, NOW())"));

        $parameters = array();

	foreach ($handlers as $handler) {
            $parameters []= serialize($handler);
            $parameters []= (string) $queue;
            $parameters []= $run_at;
        }
        $affected = self::runUpdate($sql, $parameters);

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new jobs", self::ERROR);
            return false;
        }

        if ($affected != count($handlers))
            self::log("[JOB] failed to enqueue some new jobs", self::ERROR);

        return true;
    }

	/**
     * Returns the general status of the jobs table.
     *
     * @param string $queue The queue of which to see the status for.
     *
     * @return array Information about the status.
     */
    public static function status($queue = "default") {
        $rs = self::runQuery("

	SELECT COUNT(*) as total, COUNT(failed_at) as failed, COUNT(locked_at) as locked
            FROM `" . self::$jobsTable . "`
            WHERE queue = ?
        ", array($queue));
        $rs = $rs[0];

        $failed = $rs["failed"];
        $locked = $rs["locked"];
        $total  = $rs["total"];
        $outstanding = $total - $locked - $failed;

        return array(
            "outstanding" => $outstanding,
            "locked" => $locked,
            "failed" => $failed,
            "total"  => $total
        );
    }
}
