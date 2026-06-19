'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

const { getPool } = require('../db');
const store = require('./store');
const { CsvStreamWriter } = require('../utils/csv');

/* ============================ 基础配置 ============================ */

const REPORT_DIR = process.env.REPORT_DIR || path.join(process.cwd(), 'data', 'reports');
if (!fs.existsSync(REPORT_DIR)) fs.mkdirSync(REPORT_DIR, { recursive: true });

const VALID_REPORT_TYPES = new Set([
  'session_detail',
  'revenue_cross',
  'occupancy_summary',
]);

const TASK_STATUS = Object.freeze({
  PENDING: 'PENDING',
  RUNNING: 'RUNNING',
  DONE: 'DONE',
  FAILED: 'FAILED',
});

/* ============================ task_key 生成（用于任务复用） ============================ */

/**
 * 根据报表类型 + 参数生成稳定的 task_key。
 * 同类型同参数的重复请求复用同一份任务，不重复跑。
 */
function buildTaskKey(taskType, params) {
  const stableStr = JSON.stringify(params || {}, Object.keys(params || {}).sort());
  const hash = crypto.createHash('sha256').update(`${taskType}::${stableStr}`).digest('hex').slice(0, 16);
  return `${taskType}_${hash}`;
}

function validateParams(taskType, params) {
  const p = params || {};
  if (!p.startDate || !p.endDate) {
    throw Object.assign(new Error('startDate 和 endDate 必填'), { statusCode: 400 });
  }
  const sd = new Date(p.startDate);
  const ed = new Date(p.endDate);
  if (Number.isNaN(sd.getTime()) || Number.isNaN(ed.getTime())) {
    throw Object.assign(new Error('日期格式非法'), { statusCode: 400 });
  }
  if (ed < sd) {
    throw Object.assign(new Error('endDate 不能早于 startDate'), { statusCode: 400 });
  }
  if (p.lotId !== undefined && p.lotId !== null && p.lotId !== '') {
    const n = Number(p.lotId);
    if (!Number.isInteger(n) || n <= 0) {
      throw Object.assign(new Error('lotId 必须是正整数'), { statusCode: 400 });
    }
  }
  return {
    ...p,
    startDate: `${sd.getFullYear()}-${pad(sd.getMonth() + 1)}-${pad(sd.getDate())}`,
    endDate: `${ed.getFullYear()}-${pad(ed.getMonth() + 1)}-${pad(ed.getDate())}`,
    lotId: p.lotId ? Number(p.lotId) : null,
    district: p.district || null,
    onlyPaid: !!p.onlyPaid,
  };
}

function pad(n) { return String(n).padStart(2, '0'); }

/* ============================ 任务调度（内存队列，单 worker） ============================ */

const taskQueue = [];
let workerRunning = false;
let inFlightTaskId = null;

async function scheduleNext() {
  if (workerRunning) return;
  if (taskQueue.length === 0) return;
  const taskId = taskQueue.shift();
  workerRunning = true;
  inFlightTaskId = taskId;
  try {
    await runTask(taskId);
  } catch (e) {
    // 兜底：任何未捕获的异常都记到任务表里
    try {
      await store.updateReportTask(taskId, {
        status: TASK_STATUS.FAILED,
        errorMessage: e && e.message ? e.message.slice(0, 512) : '未知错误',
        finishedAt: new Date(),
      });
    } catch (_) { /* ignore */ }
  } finally {
    workerRunning = false;
    inFlightTaskId = null;
    setImmediate(scheduleNext);
  }
}

function enqueue(taskId) {
  if (!taskQueue.includes(taskId) && inFlightTaskId !== taskId) {
    taskQueue.push(taskId);
    setImmediate(scheduleNext);
  }
}

/**
 * 启动时把库里还在 RUNNING 的任务标回 PENDING 并重跑（应对进程重启场景）。
 */
async function bootstrapResume() {
  try {
    const stale = await store.listReportTasks({ status: TASK_STATUS.RUNNING });
    for (const t of stale) {
      await store.updateReportTask(t.id, { status: TASK_STATUS.PENDING });
      enqueue(t.id);
    }
  } catch (_) { /* ignore */ }
}

/* ============================ 提交任务 ============================ */

/**
 * 提交报表导出任务。
 * 若同参数任务已存在且已完成，则直接复用；
 * 若正在跑/队列中，则返回同一个任务；
 * 否则新建一条任务并丢进调度队列。
 */
async function submitExportTask({ taskType, params, createdBy }) {
  if (!VALID_REPORT_TYPES.has(taskType)) {
    throw Object.assign(new Error(`不支持的报表类型: ${taskType}`), { statusCode: 400 });
  }
  const cleanParams = validateParams(taskType, params);
  const taskKey = buildTaskKey(taskType, cleanParams);

  let existing = await store.getReportTaskByKey(taskKey);
  if (existing) {
    // 已完成 -> 直接复用
    if (existing.status === TASK_STATUS.DONE) return existing;
    // 正在跑 / 队列中 -> 直接返回同一条，不重复
    if (existing.status === TASK_STATUS.RUNNING || existing.status === TASK_STATUS.PENDING) {
      enqueue(existing.id);
      return existing;
    }
    // FAILED -> 删掉重跑
    try {
      if (existing.filePath && fs.existsSync(existing.filePath)) {
        try { fs.unlinkSync(existing.filePath); } catch (_) { /* ignore */ }
      }
    } catch (_) { /* ignore */ }
    await getPool().query('DELETE FROM report_tasks WHERE id = ?', [existing.id]);
  }

  const task = await store.createReportTask({
    taskKey,
    taskType,
    params: cleanParams,
    createdBy,
  });
  enqueue(task.id);
  return task;
}

/* ============================ 执行任务 ============================ */

async function runTask(taskId) {
  const task = await store.getReportTaskById(taskId);
  if (!task) return;
  if (task.status === TASK_STATUS.DONE) return;

  await store.updateReportTask(taskId, {
    status: TASK_STATUS.RUNNING,
    startedAt: new Date(),
    processedCount: 0,
    errorMessage: null,
  });

  const pool = getPool();
  const p = task.params;
  const startTs = `${p.startDate} 00:00:00`;
  const endTs = `${p.endDate} 23:59:59.999`;

  // 1) 先估算总量
  const totalCount = await estimateCount(pool, task.taskType, p, startTs, endTs);
  await store.updateReportTask(taskId, { totalCount });

  // 2) 准备文件
  const fileName = buildFileName(task);
  const filePath = path.join(REPORT_DIR, fileName);

  try {
    let processed = 0;
    const progressReporter = makeProgressReporter(taskId, totalCount);

    if (task.taskType === 'session_detail') {
      processed = await runSessionDetail(pool, filePath, p, startTs, endTs, progressReporter);
    } else if (task.taskType === 'revenue_cross') {
      processed = await runRevenueCross(pool, filePath, p, startTs, endTs, progressReporter);
    } else if (task.taskType === 'occupancy_summary') {
      processed = await runOccupancySummary(pool, filePath, p, startTs, endTs, progressReporter);
    }

    const stat = fs.statSync(filePath);
    await store.updateReportTask(taskId, {
      status: TASK_STATUS.DONE,
      fileName,
      filePath,
      fileSizeBytes: stat.size,
      processedCount: processed,
      totalCount: Math.max(totalCount, processed),
      finishedAt: new Date(),
    });
  } catch (e) {
    if (fs.existsSync(filePath)) {
      try { fs.unlinkSync(filePath); } catch (_) { /* ignore */ }
    }
    await store.updateReportTask(taskId, {
      status: TASK_STATUS.FAILED,
      errorMessage: e && e.message ? e.message.slice(0, 512) : '导出失败',
      finishedAt: new Date(),
    });
    throw e;
  }
}

function makeProgressReporter(taskId, total) {
  let lastUpdate = 0;
  let current = 0;
  return async (processed) => {
    current = processed;
    const now = Date.now();
    if (now - lastUpdate < 1000 && processed < total) return; // 最多每秒更新一次
    lastUpdate = now;
    try {
      await store.updateReportTask(taskId, { processedCount: current });
    } catch (_) { /* ignore */ }
  };
}

/* ============================ 报表具体实现 ============================ */

async function estimateCount(pool, taskType, p, startTs, endTs) {
  const { where, params } = buildWhere(p, startTs, endTs);
  if (taskType === 'session_detail' || taskType === 'revenue_cross') {
    const [[row]] = await pool.query(
      `SELECT COUNT(*) AS n FROM parking_sessions s LEFT JOIN parking_lots l ON l.id = s.lot_id ${where}`,
      params,
    );
    return Number(row.n);
  }
  // occupancy_summary 的行数 = 停车场数 * 小时数 * 天数
  const days = Math.max(1, Math.ceil((new Date(endTs) - new Date(startTs)) / 86400000) + 1);
  const [[lrow]] = await pool.query('SELECT COUNT(*) AS n FROM parking_lots');
  return Number(lrow.n) * days * 24;
}

function buildWhere(p, startTs, endTs) {
  const where = [];
  const params = [];
  // 对 session_detail，按 exit_time 时间窗；若 exit_time 为空（在场中）则按 enter_time
  where.push(`(
    (s.exit_time BETWEEN ? AND ?)
    OR (s.exit_time IS NULL AND s.enter_time BETWEEN ? AND ?)
  )`);
  params.push(startTs, endTs, startTs, endTs);
  if (p.lotId) { where.push('s.lot_id = ?'); params.push(p.lotId); }
  if (p.district) { where.push('l.district = ?'); params.push(p.district); }
  if (p.onlyPaid) { where.push('s.paid = 1'); }
  return { where: `WHERE ${where.join(' AND ')}`, params };
}

/* ---------- 1. 停车明细大报表 ---------- */

async function runSessionDetail(pool, filePath, p, startTs, endTs, onProgress) {
  const headers = [
    '记录ID', '车牌号', '车主姓名', '联系电话', '车型',
    '停车场编号', '停车场名称', '区域', '车位编号',
    '入场时间', '出场时间', '停车时长(分钟)',
    '费用(元)', '支付渠道', '支付状态', '记录状态',
  ];
  const writer = new CsvStreamWriter(filePath, headers);
  const { where, params } = buildWhere(p, startTs, endTs);
  const BATCH = 2000;
  let lastId = 0;
  let total = 0;
  while (true) {
    const [rows] = await pool.query(
      `SELECT s.*, l.code AS lot_code, l.name AS lot_name, l.district,
              ps.code AS space_code,
              COALESCE(v.owner_name, '') AS owner_name,
              COALESCE(v.phone, '') AS phone,
              COALESCE(v.vehicle_type, 'UNKNOWN') AS vehicle_type
       FROM parking_sessions s
       LEFT JOIN parking_lots l ON l.id = s.lot_id
       LEFT JOIN parking_spaces ps ON ps.id = s.space_id
       LEFT JOIN vehicles v ON v.plate_no = s.plate_no
       ${where} AND s.id > ?
       ORDER BY s.id ASC
       LIMIT ?`,
      [...params, lastId, BATCH],
    );
    if (rows.length === 0) break;
    for (const r of rows) {
      const durMin = r.exit_time
        ? Math.max(0, Math.round((new Date(r.exit_time) - new Date(r.enter_time)) / 60000))
        : Math.round((Date.now() - new Date(r.enter_time).getTime()) / 60000);
      writer.writeRowSync([
        r.id,
        r.plate_no,
        r.owner_name,
        r.phone,
        r.vehicle_type,
        r.lot_code,
        r.lot_name,
        r.district,
        r.space_code || '',
        r.enter_time,
        r.exit_time || '',
        durMin,
        (r.fee_cents / 100).toFixed(2),
        r.payment_channel || 'NONE',
        r.paid ? '已支付' : '未支付',
        r.status,
      ]);
      lastId = r.id;
      total += 1;
    }
    if (onProgress) await onProgress(total);
    if (rows.length < BATCH) break;
  }
  writer.end();
  return total;
}

/* ---------- 2. 营收多维交叉汇总 ---------- */

async function runRevenueCross(pool, filePath, p, startTs, endTs, onProgress) {
  const headers = [
    '日期', '区域', '停车场ID', '停车场名称',
    '支付渠道', '车型',
    '车次', '营收(分)', '营收(元)', '平均停车时长(分钟)',
  ];
  const writer = new CsvStreamWriter(filePath, headers);
  const where = [];
  const params = [];
  where.push('s.exit_time BETWEEN ? AND ?');
  params.push(startTs, endTs);
  where.push("s.status = 'FINISHED' AND s.paid = 1");
  if (p.lotId) { where.push('s.lot_id = ?'); params.push(p.lotId); }
  if (p.district) { where.push('l.district = ?'); params.push(p.district); }
  const whereClause = `WHERE ${where.join(' AND ')}`;

  const [rows] = await pool.query(
    `SELECT DATE_FORMAT(s.exit_time, '%Y-%m-%d') AS day,
            l.district,
            s.lot_id,
            l.name AS lot_name,
            s.payment_channel,
            COALESCE(v.vehicle_type, 'UNKNOWN') AS vehicle_type,
            COUNT(*) AS session_count,
            SUM(s.fee_cents) AS revenue_cents,
            SUM(TIMESTAMPDIFF(MINUTE, s.enter_time, s.exit_time)) AS duration_min_sum
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     LEFT JOIN vehicles v ON v.plate_no = s.plate_no
     ${whereClause}
     GROUP BY day, l.district, s.lot_id, s.payment_channel, v.vehicle_type
     ORDER BY day, l.district, s.lot_id, s.payment_channel, v.vehicle_type`,
    params,
  );

  let total = 0;
  for (let i = 0; i < rows.length; i += 1) {
    const r = rows[i];
    const sc = Number(r.session_count) || 0;
    const rc = Number(r.revenue_cents) || 0;
    const dm = Number(r.duration_min_sum) || 0;
    writer.writeRowSync([
      r.day,
      r.district,
      r.lot_id,
      r.lot_name,
      r.payment_channel || 'NONE',
      r.vehicle_type,
      sc,
      rc,
      (rc / 100).toFixed(2),
      sc > 0 ? Number((dm / sc).toFixed(2)) : 0,
    ]);
    total += 1;
    if (onProgress && (i % 1000 === 0 || i === rows.length - 1)) await onProgress(total);
  }
  writer.end();
  return total;
}

/* ---------- 3. 占用率汇总 ---------- */

async function runOccupancySummary(pool, filePath, p, startTs, endTs, onProgress) {
  const headers = [
    '日期', '小时', '区域', '停车场ID', '停车场名称',
    '总车位数', '入场车次', '出场车次', '累计在场(近似)',
  ];
  const writer = new CsvStreamWriter(filePath, headers);

  const whereL = []; const paramsL = [];
  if (p.lotId) { whereL.push('id = ?'); paramsL.push(p.lotId); }
  if (p.district) { whereL.push('district = ?'); paramsL.push(p.district); }
  const clauseL = whereL.length ? `WHERE ${whereL.join(' AND ')}` : '';
  const [lots] = await pool.query(`SELECT * FROM parking_lots ${clauseL} ORDER BY id`, paramsL);

  // 先按停车场 + 日期 + 小时聚合入场和出场
  const where = []; const params = [];
  params.push(startTs, endTs, startTs, endTs);
  if (p.lotId) { where.push('s.lot_id = ?'); params.push(p.lotId); }
  if (p.district) { where.push('l.district = ?'); params.push(p.district); }
  const clause = where.length ? `AND ${where.join(' AND ')}` : '';

  const [ioRows] = await pool.query(
    `SELECT s.lot_id, l.district, l.name AS lot_name,
            DATE_FORMAT(COALESCE(s.exit_time, s.enter_time), '%Y-%m-%d') AS day,
            HOUR(COALESCE(s.exit_time, s.enter_time)) AS h,
            SUM(CASE WHEN s.enter_time BETWEEN ? AND ? THEN 1 ELSE 0 END) AS entry_count,
            SUM(CASE WHEN s.exit_time BETWEEN ? AND ? AND s.status = 'FINISHED' THEN 1 ELSE 0 END) AS exit_count
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     WHERE (
       (s.exit_time IS NOT NULL AND s.exit_time BETWEEN ? AND ?)
       OR (s.exit_time IS NULL AND s.enter_time BETWEEN ? AND ?)
     ) ${clause}
     GROUP BY s.lot_id, l.district, l.name, day, h
     ORDER BY day, h, s.lot_id`,
    [startTs, endTs, startTs, endTs, startTs, endTs, startTs, endTs, ...params.slice(2)],
  );

  // 把结果放进查找表
  const lookup = new Map();
  for (const r of ioRows) {
    lookup.set(`${r.lot_id}|${r.day}|${r.h}`, r);
  }

  // 生成笛卡尔积（所有停车场 × 所有日期 × 24小时）保证零值行
  const sd = new Date(startTs);
  const ed = new Date(endTs);
  const days = [];
  for (let d = new Date(sd); d <= ed; d.setDate(d.getDate() + 1)) {
    days.push(`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`);
  }

  let total = 0;
  let processed = 0;
  const totalExpected = lots.length * days.length * 24;

  for (const day of days) {
    const runningByLot = new Map();
    for (const lot of lots) runningByLot.set(lot.id, 0);

    for (let h = 0; h < 24; h += 1) {
      for (const lot of lots) {
        const r = lookup.get(`${lot.id}|${day}|${h}`);
        const en = r ? (Number(r.entry_count) || 0) : 0;
        const ex = r ? (Number(r.exit_count) || 0) : 0;
        const run = (runningByLot.get(lot.id) || 0) + en - ex;
        runningByLot.set(lot.id, run);
        writer.writeRowSync([
          day,
          h,
          r ? r.district : lot.district,
          lot.id,
          r ? r.lot_name : lot.name,
          lot.total_spaces,
          en,
          ex,
          Math.max(0, run),
        ]);
        total += 1;
        processed += 1;
        if (onProgress && (processed % 5000 === 0 || processed === totalExpected)) {
          await onProgress(processed);
        }
      }
    }
  }

  writer.end();
  return total;
}

/* ============================ 文件名 ============================ */

function buildFileName(task) {
  const now = new Date();
  const ts = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
  const map = {
    session_detail: '停车明细报表',
    revenue_cross: '营收交叉汇总报表',
    occupancy_summary: '占用率时段汇总报表',
  };
  const label = map[task.taskType] || task.taskType;
  const range = `${task.params.startDate}_${task.params.endDate}`;
  return `${label}_${range}_${ts}_${task.id}.csv`;
}

/* ============================ 查询/下载 ============================ */

async function getTaskStatus(taskId) {
  return store.getReportTaskById(taskId);
}

async function listTasks({ status, taskType, limit } = {}) {
  return store.listReportTasks({ status, taskType, limit });
}

function resolveFilePath(task) {
  if (!task || !task.filePath) return null;
  if (!fs.existsSync(task.filePath)) return null;
  return task.filePath;
}

/* ============================ 导出 ============================ */

process.nextTick(bootstrapResume);

module.exports = {
  REPORT_DIR,
  VALID_REPORT_TYPES,
  TASK_STATUS,
  buildTaskKey,
  submitExportTask,
  getTaskStatus,
  listTasks,
  resolveFilePath,
};
