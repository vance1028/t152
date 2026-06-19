'use strict';

const express = require('express');
const fs = require('fs');
const path = require('path');

const { authRequired, requireRole } = require('../auth');
const { sendData, sendError, parseId } = require('../utils/http');
const reports = require('../data/reports');

const router = express.Router();
router.use(authRequired);

/* GET /api/reports/types —— 列出支持的报表类型。 */
router.get('/types', (req, res) => {
  return sendData(res, 200, {
    types: [
      { key: 'session_detail', name: '停车明细报表', desc: '按时间段导出全量停车记录明细（含车场/区域/筛选）' },
      { key: 'revenue_cross', name: '营收交叉汇总报表', desc: '按日期 × 区域 × 停车场 × 支付渠道 × 车型多维交叉汇总营收' },
      { key: 'occupancy_summary', name: '占用率时段汇总报表', desc: '按日 × 小时 × 停车场维度导出入场/出场/在场累计，零值行完整' },
    ],
  });
});

/* GET /api/reports/tasks —— 任务列表。
   query: status?, taskType?, limit?
*/
router.get('/tasks', async (req, res, next) => {
  try {
    const { status, taskType, limit } = req.query;
    const opts = { status, taskType };
    if (limit !== undefined) opts.limit = Math.max(1, Math.min(500, Number(limit)));
    return sendData(res, 200, await reports.listTasks(opts));
  } catch (e) { return next(e); }
});

/* POST /api/reports/tasks —— 提交导出任务。
   body: { taskType, params: { startDate, endDate, lotId?, district?, onlyPaid? } }
*/
router.post('/tasks', requireRole('ADMIN', 'OPERATOR', 'VIEWER'), async (req, res, next) => {
  try {
    const { taskType, params } = req.body || {};
    if (!taskType) return sendError(res, 400, 'taskType 必填');
    if (!reports.VALID_REPORT_TYPES.has(taskType)) {
      return sendError(res, 400, `不支持的报表类型: ${taskType}`);
    }
    const createdBy = req.user && req.user.id;
    const task = await reports.submitExportTask({ taskType, params, createdBy });
    return sendData(res, 202, task);
  } catch (e) { return next(e); }
});

/* GET /api/reports/tasks/:id —— 查任务进度和状态。 */
router.get('/tasks/:id', async (req, res, next) => {
  try {
    const id = parseId(req.params.id);
    const task = await reports.getTaskStatus(id);
    if (!task) return sendError(res, 404, '任务不存在');
    return sendData(res, 200, task);
  } catch (e) { return next(e); }
});

/* GET /api/reports/tasks/:id/download —— 下载任务结果文件。 */
router.get('/tasks/:id/download', async (req, res, next) => {
  try {
    const id = parseId(req.params.id);
    const task = await reports.getTaskStatus(id);
    if (!task) return sendError(res, 404, '任务不存在');
    if (task.status !== reports.TASK_STATUS.DONE) {
      return sendError(res, 409, `任务尚未完成（当前状态: ${task.status}）`);
    }
    const filePath = reports.resolveFilePath(task);
    if (!filePath) {
      return sendError(res, 410, '导出文件已被清理，请重新提交任务');
    }
    const fileName = task.fileName || path.basename(filePath);
    const safeFileName = encodeURIComponent(fileName);
    // Excel 能识别的文件名响应头
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${safeFileName}"; filename*=UTF-8''${safeFileName}`);
    res.setHeader('Content-Length', String(task.fileSizeBytes || fs.statSync(filePath).size));
    res.setHeader('Cache-Control', 'private, max-age=0');
    const stream = fs.createReadStream(filePath);
    stream.on('error', (err) => next(err));
    stream.pipe(res);
  } catch (e) { return next(e); }
});

module.exports = router;
