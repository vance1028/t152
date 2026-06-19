'use strict';

const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');

/**
 * MySQL 连接管理（mysql2/promise 连接池）。
 * 全程 utf8mb4，确保中文不乱码。
 */

const DB_CONFIG = {
  host: process.env.DB_HOST || '127.0.0.1',
  port: Number(process.env.DB_PORT) || 13366,
  user: process.env.DB_USER || 'park',
  password: process.env.DB_PASSWORD || 'parkpass',
  database: process.env.DB_NAME || 'parking',
  charset: 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 10,
  namedPlaceholders: true,
};

let pool = null;

function getPool() {
  if (!pool) {
    pool = mysql.createPool(DB_CONFIG);
  }
  return pool;
}

/**
 * 确保表结构存在（读取 db/schema.sql 执行）。
 * 用一个开启 multipleStatements 的临时连接执行，执行完关闭。
 * 然后用 ALTER TABLE 迁移已有表，补上新增列和索引（IF NOT EXISTS 不处理已有表）。
 */
async function ensureSchema() {
  const schemaPath = path.join(__dirname, '..', 'db', 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  const conn = await mysql.createConnection({ ...DB_CONFIG, multipleStatements: true });
  try {
    await conn.query(sql);
    await migrateExistingTables(conn);
  } finally {
    await conn.end();
  }
}

async function columnExists(conn, table, column) {
  const [rows] = await conn.query(
    `SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [DB_CONFIG.database, table, column],
  );
  return rows[0].n > 0;
}

async function indexExists(conn, table, index) {
  const [rows] = await conn.query(
    `SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.STATISTICS
     WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?`,
    [DB_CONFIG.database, table, index],
  );
  return rows[0].n > 0;
}

async function migrateExistingTables(conn) {
  // parking_sessions: payment_channel 列
  if (!(await columnExists(conn, 'parking_sessions', 'payment_channel'))) {
    await conn.query(
      `ALTER TABLE parking_sessions
       ADD COLUMN payment_channel VARCHAR(32) NOT NULL DEFAULT 'NONE' AFTER paid`,
    );
  }
  // parking_sessions: 索引
  if (!(await indexExists(conn, 'parking_sessions', 'idx_session_lot_enter'))) {
    await conn.query(
      `CREATE INDEX idx_session_lot_enter ON parking_sessions (lot_id, enter_time)`,
    );
  }
  if (!(await indexExists(conn, 'parking_sessions', 'idx_session_lot_exit'))) {
    await conn.query(
      `CREATE INDEX idx_session_lot_exit ON parking_sessions (lot_id, exit_time)`,
    );
  }
  if (!(await indexExists(conn, 'parking_sessions', 'idx_session_exit_paid'))) {
    await conn.query(
      `CREATE INDEX idx_session_exit_paid ON parking_sessions (exit_time, paid)`,
    );
  }
  // report_tasks 表（schema.sql 里是 IF NOT EXISTS，首次跑会建）
}

/** 清空所有业务数据（测试用）。 */
async function resetAll() {
  const conn = await getPool().getConnection();
  try {
    await conn.query('SET FOREIGN_KEY_CHECKS = 0');
    for (const t of ['report_tasks', 'parking_sessions', 'parking_spaces', 'vehicles', 'parking_lots', 'users']) {
      await conn.query(`TRUNCATE TABLE ${t}`);
    }
    await conn.query('SET FOREIGN_KEY_CHECKS = 1');
  } finally {
    conn.release();
  }
}

/** 等待数据库可连接（最多重试若干次），用于启动时等容器就绪。 */
async function waitForDb(retries = 30, delayMs = 1000) {
  for (let i = 0; i < retries; i += 1) {
    try {
      const conn = await mysql.createConnection({ ...DB_CONFIG, database: undefined });
      await conn.end();
      return true;
    } catch (e) {
      await new Promise((r) => setTimeout(r, delayMs));
    }
  }
  throw new Error('数据库连接超时');
}

async function close() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

module.exports = { getPool, ensureSchema, resetAll, waitForDb, close, DB_CONFIG };
