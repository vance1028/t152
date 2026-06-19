'use strict';

const express = require('express');
const { authRequired } = require('../auth');
const { sendData, sendError, parseId } = require('../utils/http');
const analytics = require('../data/analytics');

const router = express.Router();
router.use(authRequired);

/* GET /api/analytics/overview —— 运营大屏总览聚合。 */
router.get('/overview', async (req, res, next) => {
  try {
    const { date } = req.query;
    const data = await analytics.getOverview({ date });
    return sendData(res, 200, data);
  } catch (e) { return next(e); }
});

/* GET /api/analytics/drilldown —— 下钻趋势聚合。
   query: lotId?, district?, granularity=day|hour|week, startDate, endDate
*/
router.get('/drilldown', async (req, res, next) => {
  try {
    const { lotId, district, granularity, startDate, endDate } = req.query;
    const opts = { granularity, startDate, endDate };
    if (lotId !== undefined && lotId !== '') opts.lotId = parseId(lotId);
    if (district) opts.district = district;
    const data = await analytics.getDrilldown(opts);
    return sendData(res, 200, data);
  } catch (e) { return next(e); }
});

/* GET /api/analytics/realtime —— 实时在场明细（对账用）。
   query: lotId?, district?, page, pageSize
*/
router.get('/realtime', async (req, res, next) => {
  try {
    const { lotId, district, page, pageSize } = req.query;
    const opts = { page: page ? Number(page) : undefined, pageSize: pageSize ? Number(pageSize) : undefined };
    if (lotId !== undefined && lotId !== '') opts.lotId = parseId(lotId);
    if (district) opts.district = district;
    const data = await analytics.listRealtimeSessions(opts);
    return sendData(res, 200, data);
  } catch (e) { return next(e); }
});

module.exports = router;
