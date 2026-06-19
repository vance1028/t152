'use strict';

const fs = require('fs');
const path = require('path');

/* ============================ CSV 转义与编码 ============================ */

/**
 * CSV 单个字段转义。
 * 规则：
 *   - Date 单独处理：toISOString()，不要走 JSON.stringify（否则会多出首尾双引号）
 *   - 其他对象用 JSON.stringify
 *   - 若字段包含逗号、双引号、换行符（\n 或 \r），则必须用双引号包起来
 *   - 内部的双引号要转成两个双引号
 *   - 空值统一返回空字符串
 */
function escapeCsvField(value) {
  if (value === null || value === undefined) return '';
  let s;
  if (value instanceof Date) {
    s = value.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  } else if (typeof value === 'object') {
    try { s = JSON.stringify(value); } catch (_) { s = String(value); }
  } else {
    s = String(value);
  }
  if (s === '') return '';
  const needsQuote = /[",\n\r]/.test(s);
  if (needsQuote) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

/**
 * 将一行数组拼成 CSV 行字符串。
 */
function toCsvRow(fields) {
  return fields.map(escapeCsvField).join(',') + '\r\n';
}

/**
 * 生成 UTF-8 BOM，Excel 打开中文 CSV 不乱码的关键。
 */
const UTF8_BOM = Buffer.from([0xEF, 0xBB, 0xBF]);

/* ============================ 流式 CSV 写入器 ============================ */

/**
 * 大文件流式 CSV 写入器。
 * 用法：
 *   const w = new CsvStreamWriter(filePath, ['列A', '列B']);
 *   await w.writeRow(['值1', '值2']);
 *   await w.end();
 */
class CsvStreamWriter {
  constructor(filePath, headers) {
    this.filePath = filePath;
    this.headers = headers;
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    this.fd = fs.openSync(filePath, 'w');
    fs.writeSync(this.fd, UTF8_BOM);
    this.writeRowSync(headers);
    this.rowCount = 0;
  }

  writeRowSync(fields) {
    const buf = Buffer.from(toCsvRow(fields), 'utf8');
    fs.writeSync(this.fd, buf);
    this.rowCount += 1;
  }

  async writeRow(fields) {
    return this.writeRowSync(fields);
  }

  flush() {
    try { fs.fsyncSync(this.fd); } catch (_) { /* ignore */ }
  }

  end() {
    this.flush();
    try { fs.closeSync(this.fd); } catch (_) { /* ignore */ }
    this.fd = null;
    const stat = fs.statSync(this.filePath);
    return { rowCount: this.rowCount, sizeBytes: stat.size };
  }
}

/* ============================ 全量生成 CSV（内存友好型，基于游标） ============================ */

/**
 * 分批次流式写入 CSV，避免把大结果集一次性塞进内存。
 * @param {string} filePath - 输出文件路径
 * @param {string[]} headers - 表头数组（中文列名）
 * @param {Function} fetchBatch - async (lastId, batchSize) => { rows: any[], done: boolean, lastId: number }
 *                                 每一行是一个数组，顺序与 headers 对应
 * @param {Object} [opts]
 * @param {number} [opts.batchSize=1000]
 * @param {Function} [opts.onProgress] - (processed, totalEstimate) => void
 */
async function writeCsvBatched(filePath, headers, fetchBatch, opts = {}) {
  const batchSize = opts.batchSize || 1000;
  const writer = new CsvStreamWriter(filePath, headers);
  let processed = 0;
  let lastId = 0;
  let estimated = 0;

  try {
    while (true) {
      const result = await fetchBatch(lastId, batchSize);
      if (!result || !result.rows || result.rows.length === 0) break;
      for (const row of result.rows) writer.writeRowSync(row);
      processed += result.rows.length;
      lastId = result.lastId || (lastId + result.rows.length);
      estimated = Math.max(estimated, processed + (result.remaining || 0));
      if (opts.onProgress) opts.onProgress(processed, estimated);
      if (result.done) break;
    }
  } finally {
    writer.end();
  }

  return {
    rowCount: writer.rowCount - 1,
    sizeBytes: fs.statSync(filePath).size,
  };
}

module.exports = {
  escapeCsvField,
  toCsvRow,
  UTF8_BOM,
  CsvStreamWriter,
  writeCsvBatched,
};
