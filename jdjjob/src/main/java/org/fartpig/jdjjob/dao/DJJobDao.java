package org.fartpig.jdjjob.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.fartpig.jdjjob.DJBase;
import org.fartpig.jdjjob.Job;

public class DJJobDao {

	private static String ID_COLUMN = "`id`";
	private static String COLUMNS = "`handler`, `queue`, `attempts`, `run_at`, `locked_at`, `locked_by`, `failed_at`, `error`, `created_at`";

	public boolean save(Job job) {
		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());

		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(DJBase.jobsTable);
		sb.append(" (");
		sb.append(COLUMNS);
		sb.append(") VALUE (? , ? ,? ,? ,? ,? ,? ,? , NOW())");

		int num = 0;
		try {
			num = qr.update(sb.toString(), job.getHandler(), job.getQueue(), job.getAttempts(), job.getRunAt(),
					job.getLockedAt(), job.getLockedBy(), job.getFailedAt(), job.getError());
			if (num != 0) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;
	}

	public boolean delete(Job job) {

		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());

		StringBuilder sb = new StringBuilder();
		sb.append("DELETE FROM ");
		sb.append(DJBase.jobsTable);
		sb.append("  WHERE ");
		sb.append(ID_COLUMN);
		sb.append("=?");

		int num = 0;
		try {
			num = qr.update(sb.toString(), job.getId().longValue());
			if (num != 0) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;
	}

	public List<Object[]> executeQuery(String sql, List<Object> args) {
		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());
		try {
			return qr.query(sql, new ArrayListHandler(), args.toArray());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Collections.<Object[]>emptyList();
	}

	public boolean execute(String updateSql, List<Object> args) {

		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());
		try {
			qr.update(updateSql, args.toArray());
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean update(Job job) {

		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());

		StringBuilder sb = new StringBuilder();
		sb.append("UPDATE ");
		sb.append(DJBase.jobsTable);
		sb.append(" SET  `handler` = ? ,");
		sb.append(" `queue` = ? ,");
		sb.append(" `attempts` = ? ,");
		sb.append(" `run_at` = ? ,");
		sb.append(" `locked_at` = ? ,");
		sb.append(" `locked_by` = ? ,");
		sb.append(" `failed_at` = ? ,");
		sb.append(" `error` = ? ");
		sb.append(" WHERE ");
		sb.append(ID_COLUMN);
		sb.append("=?");

		int num = 0;
		try {
			num = qr.update(sb.toString(), job.getHandler(), job.getQueue(), job.getAttempts(), job.getRunAt(),
					job.getLockedAt(), job.getLockedBy(), job.getFailedAt(), job.getError(), job.getId().longValue());
			if (num != 0) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public List<Job> findByCondition(String whereSql, String orderSql, List<Object> args) {
		QueryRunner qr = new QueryRunner(DBUtils.getDataSource());
		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		sb.append(ID_COLUMN);
		sb.append(" ,");
		sb.append(COLUMNS);
		sb.append(" FROM ");
		sb.append(DJBase.jobsTable);
		sb.append(" WHERE ");
		sb.append(whereSql);
		if (orderSql != null) {
			sb.append(" ORDER BY ");
			sb.append(orderSql);
		}

		try {
			return qr.query(sb.toString(), new BeanListHandler<Job>(Job.class), args.toArray());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return Collections.<Job>emptyList();
	}

	public Job loadByJobId(long jobId) {
		List<Object> args = new ArrayList<Object>();
		args.add(jobId);
		List<Job> jobs = findByCondition("id = ? ", null, args);
		return jobs.size() > 0 ? jobs.get(0) : null;
	}

}