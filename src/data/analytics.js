'use strict';

const { getPool } = require('../db');
const store = require('./store');

/* ============================ 工具函数 ============================ */

function todayRange() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return {
    start: `${y}-${m}-${d} 00:00:00`,
    end: `${y}-${m}-${d} 23:59:59.999`,
  };
}

function pad(n) { return String(n).padStart(2, '0'); }

function parseDate(s) {
  if (s instanceof Date) return s;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) throw new Error(`非法日期: ${s}`);
  return d;
}

/** 生成完整的24小时空桶，用于热力图和趋势图的零态填充。 */
function emptyHourBuckets() {
  const arr = new Array(24);
  for (let h = 0; h < 24; h += 1) arr[h] = { hour: h, count: 0 };
  return arr;
}

/** 生成按停车场维度的完整空结构，确保空数据时有完整零值。 */
function buildEmptyLotStats(lots) {
  const result = {};
  for (const lot of lots) {
    result[lot.id] = {
      lotId: lot.id,
      lotName: lot.name,
      district: lot.district,
      totalSpaces: lot.totalSpaces,
      occupiedNow: 0,
      occupancyRate: 0,
      entryCount: 0,
      exitCount: 0,
      revenueCents: 0,
      turnoverRate: 0,
      avgDurationMinutes: 0,
      revenueByChannel: {},
      revenueByVehicleType: {},
      hourlyOccupancy: emptyHourBuckets(),
    };
  }
  return result;
}

/** 生成区域维度的完整空结构。 */
function buildEmptyDistrictStats(lots) {
  const districts = [...new Set(lots.map((l) => l.district))];
  const result = {};
  for (const d of districts) {
    const districtLots = lots.filter((l) => l.district === d);
    result[d] = {
      district: d,
      lotCount: districtLots.length,
      totalSpaces: districtLots.reduce((s, l) => s + l.totalSpaces, 0),
      occupiedNow: 0,
      occupancyRate: 0,
      entryCount: 0,
      exitCount: 0,
      revenueCents: 0,
      turnoverRate: 0,
      avgDurationMinutes: 0,
      revenueByChannel: {},
      revenueByVehicleType: {},
      hourlyOccupancy: emptyHourBuckets(),
    };
  }
  return result;
}

/* ============================ 总览聚合 ============================ */

/**
 * 运营大屏总览接口：一次性用聚合查询算出所有核心指标。
 * 全程单条/少量 SQL，绝对没有"先查停车场再循环查"的 N+1。
 */
async function getOverview({ date } = {}) {
  const pool = getPool();
  const range = date ? { start: `${date} 00:00:00`, end: `${date} 23:59:59.999` } : todayRange();

  const lots = await store.listLots();
  const lotIds = lots.map((l) => l.id);

  const empty = buildEmptyLotStats(lots);
  const emptyDistricts = buildEmptyDistrictStats(lots);

  const channelSet = new Set();
  const vehicleTypeSet = new Set();

  let realtimeOccupied = 0;
  let totalSpaces = 0;
  let todayEntries = 0;
  let todayExits = 0;
  let todayRevenueCents = 0;
  let totalTurnovers = 0;
  let totalFinishedSessions = 0;
  let totalDurationSeconds = 0;

  if (lotIds.length > 0) {
    const placeholders = lotIds.map(() => '?').join(',');

    /* 1) 实时在场：每个停车场的当前 PARKED 数量 —— 一把 GROUP BY */
    const [occRows] = await pool.query(
      `SELECT lot_id, COUNT(*) AS n FROM parking_sessions
       WHERE status = 'PARKED' AND lot_id IN (${placeholders})
       GROUP BY lot_id`,
      lotIds,
    );
    for (const r of occRows) {
      const s = empty[r.lot_id];
      if (s) {
        s.occupiedNow = Number(r.n);
        s.occupancyRate = s.totalSpaces > 0 ? Number((s.occupiedNow / s.totalSpaces).toFixed(4)) : 0;
        realtimeOccupied += s.occupiedNow;
      }
    }
    totalSpaces = lots.reduce((s, l) => s + l.totalSpaces, 0);

    /* 2) 今日进出场：一把按 lot_id + 类型(进/出)聚合，用 UNION ALL 一次查完 */
    const [ioRows] = await pool.query(
      `SELECT lot_id, 'ENTRY' AS kind, COUNT(*) AS n FROM parking_sessions
       WHERE enter_time BETWEEN ? AND ? AND lot_id IN (${placeholders})
       GROUP BY lot_id
       UNION ALL
       SELECT lot_id, 'EXIT' AS kind, COUNT(*) AS n FROM parking_sessions
       WHERE exit_time BETWEEN ? AND ? AND status = 'FINISHED' AND lot_id IN (${placeholders})
       GROUP BY lot_id`,
      [range.start, range.end, ...lotIds, range.start, range.end, ...lotIds],
    );
    for (const r of ioRows) {
      const s = empty[r.lot_id];
      if (!s) continue;
      if (r.kind === 'ENTRY') { s.entryCount = Number(r.n); todayEntries += s.entryCount; }
      else { s.exitCount = Number(r.n); todayExits += s.exitCount; }
    }

    /* 3) 今日营收 + 分渠道 + 分车型 + 平均时长 + 周转率因子 —— 一把 JOIN vehicles 聚合
       注意：SQL 层不能对 v.vehicle_type 做 COALESCE，否则 WITH ROLLUP 小计行的 NULL
             会被误转成 'UNKNOWN'，无法识别汇总行。空值在 JS 层处理。 */
    const [revRows] = await pool.query(
      `SELECT s.lot_id,
              s.payment_channel,
              v.vehicle_type,
              COUNT(*) AS session_count,
              SUM(s.fee_cents) AS fee_sum,
              SUM(TIMESTAMPDIFF(SECOND, s.enter_time, s.exit_time)) AS duration_sec_sum
       FROM parking_sessions s
       LEFT JOIN vehicles v ON v.plate_no = s.plate_no
       WHERE s.exit_time BETWEEN ? AND ? AND s.status = 'FINISHED' AND s.paid = 1
         AND s.lot_id IN (${placeholders})
       GROUP BY s.lot_id, s.payment_channel, v.vehicle_type
       WITH ROLLUP`,
      [range.start, range.end, ...lotIds],
    );

    for (const r of revRows) {
      const lotId = r.lot_id;
      const ch = r.payment_channel;
      const vt = r.vehicle_type;

      if (lotId == null) {
        continue;
      }
      const s = empty[lotId];
      if (!s) continue;

      /* ROLLUP 总计行（lot_id 粒度）：ch=NULL 且 vt=NULL，存总数、周转率、平均时长 */
      if (ch == null && vt == null) {
        s.revenueCents = Number(r.fee_sum) || 0;
        const finSessions = Number(r.session_count) || 0;
        const durSec = Number(r.duration_sec_sum) || 0;
        totalFinishedSessions += finSessions;
        totalDurationSeconds += durSec;
        totalTurnovers += finSessions;
        todayRevenueCents += s.revenueCents;
        if (s.totalSpaces > 0) {
          s.turnoverRate = Number((finSessions / s.totalSpaces).toFixed(4));
        }
        if (finSessions > 0) {
          s.avgDurationMinutes = Number(((durSec / finSessions) / 60).toFixed(2));
        }
        continue;
      }

      /* ROLLUP 小计行（按 lot_id + channel）：vt=NULL 但 ch≠NULL —— 跳过，避免重复累加 */
      if (vt == null) {
        continue;
      }

      /* 最底层（ch 非空且 vt 非空）：累加分渠道/分车型 */
      const vtKey = vt || 'UNKNOWN';
      const chKey = ch || 'NONE';
      channelSet.add(chKey);
      vehicleTypeSet.add(vtKey);
      const fees = Number(r.fee_sum) || 0;
      s.revenueByChannel[chKey] = (s.revenueByChannel[chKey] || 0) + fees;
      s.revenueByVehicleType[vtKey] = (s.revenueByVehicleType[vtKey] || 0) + fees;
    }

    /* 4) 热力分布：每个停车场在当天按小时的入场累计（可画热力） —— 一把聚合 */
    const [heatRows] = await pool.query(
      `SELECT lot_id, HOUR(enter_time) AS h, COUNT(*) AS n
       FROM parking_sessions
       WHERE enter_time BETWEEN ? AND ? AND lot_id IN (${placeholders})
       GROUP BY lot_id, HOUR(enter_time)`,
      [range.start, range.end, ...lotIds],
    );
    for (const r of heatRows) {
      const s = empty[r.lot_id];
      if (s && r.h >= 0 && r.h < 24) {
        s.hourlyOccupancy[r.h].count = Number(r.n);
      }
    }
  }

  /* 汇总到区域维度 */
  for (const lot of lots) {
    const s = empty[lot.id];
    const d = emptyDistricts[lot.district];
    if (!s || !d) continue;
    d.occupiedNow += s.occupiedNow;
    d.entryCount += s.entryCount;
    d.exitCount += s.exitCount;
    d.revenueCents += s.revenueCents;
    for (const ch of Object.keys(s.revenueByChannel)) {
      d.revenueByChannel[ch] = (d.revenueByChannel[ch] || 0) + s.revenueByChannel[ch];
    }
    for (const vt of Object.keys(s.revenueByVehicleType)) {
      d.revenueByVehicleType[vt] = (d.revenueByVehicleType[vt] || 0) + s.revenueByVehicleType[vt];
    }
    for (let h = 0; h < 24; h += 1) {
      d.hourlyOccupancy[h].count += s.hourlyOccupancy[h].count;
    }
  }
  for (const d of Object.values(emptyDistricts)) {
    d.occupancyRate = d.totalSpaces > 0 ? Number((d.occupiedNow / d.totalSpaces).toFixed(4)) : 0;
    if (d.totalSpaces > 0) {
      d.turnoverRate = Number((totalTurnoversPerDistrict(d, empty, lots) / d.totalSpaces).toFixed(4));
    }
  }

  const channels = [...channelSet];
  const vehicleTypes = [...vehicleTypeSet];

  const lotStats = lots.map((l) => {
    const s = empty[l.id];
    return {
      ...s,
      revenueByChannel: normalizeMap(s.revenueByChannel, channels),
      revenueByVehicleType: normalizeMap(s.revenueByVehicleType, vehicleTypes),
    };
  });

  const districtStats = Object.values(emptyDistricts).map((d) => ({
    ...d,
    revenueByChannel: normalizeMap(d.revenueByChannel, channels),
    revenueByVehicleType: normalizeMap(d.revenueByVehicleType, vehicleTypes),
  }));

  const globalRevenueByChannel = {};
  const globalRevenueByVehicleType = {};
  for (const ch of channels) {
    globalRevenueByChannel[ch] = lotStats.reduce((s, l) => s + (l.revenueByChannel[ch] || 0), 0);
  }
  for (const vt of vehicleTypes) {
    globalRevenueByVehicleType[vt] = lotStats.reduce((s, l) => s + (l.revenueByVehicleType[vt] || 0), 0);
  }

  return {
    date: range.start.slice(0, 10),
    generatedAt: new Date().toISOString(),
    realtime: {
      totalVehiclesOnSite: realtimeOccupied,
      totalSpaces,
      overallOccupancyRate: totalSpaces > 0 ? Number((realtimeOccupied / totalSpaces).toFixed(4)) : 0,
      lotsCount: lots.length,
      openLotsCount: lots.filter((l) => l.status === 'OPEN').length,
    },
    today: {
      entries: todayEntries,
      exits: todayExits,
      revenueCents: todayRevenueCents,
      revenueYuan: Number((todayRevenueCents / 100).toFixed(2)),
      turnoverRate: totalSpaces > 0 ? Number((totalTurnovers / totalSpaces).toFixed(4)) : 0,
      avgParkingMinutes: totalFinishedSessions > 0 ? Number(((totalDurationSeconds / totalFinishedSessions) / 60).toFixed(2)) : 0,
      revenueByChannel: globalRevenueByChannel,
      revenueByVehicleType: globalRevenueByVehicleType,
    },
    lots: lotStats,
    districts: districtStats,
    heatmap: buildHeatmapData(lotStats),
  };
}

function totalTurnoversPerDistrict(d, empty, lots) {
  return lots
    .filter((l) => l.district === d.district)
    .reduce((s, l) => s + (empty[l.id] ? countFinishedFromEmpty(empty[l.id]) : 0), 0);
}
function countFinishedFromEmpty(s) {
  return s.exitCount;
}

function normalizeMap(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = obj[k] || 0;
  for (const k of Object.keys(obj)) if (!keys.includes(k)) out[k] = obj[k];
  return out;
}

function buildHeatmapData(lotStats) {
  const rows = [];
  for (const l of lotStats) {
    for (const hb of l.hourlyOccupancy) {
      rows.push({
        lotId: l.lotId,
        lotName: l.lotName,
        district: l.district,
        hour: hb.hour,
        count: hb.count,
      });
    }
  }
  return rows;
}

/* ============================ 下钻接口 ============================ */

const VALID_GRANULARITIES = new Set(['hour', 'day', 'week']);

/**
 * 下钻接口：按停车场/区域 + 时间粒度查看指标趋势。
 * 全部用 SQL 聚合一把出，时间桶在 SQL 里算。
 */
async function getDrilldown({
  lotId, district, granularity = 'day', startDate, endDate,
} = {}) {
  if (!VALID_GRANULARITIES.has(granularity)) {
    throw Object.assign(new Error('granularity 只能是 hour/day/week'), { statusCode: 400 });
  }
  if (!startDate || !endDate) {
    throw Object.assign(new Error('startDate 和 endDate 必填'), { statusCode: 400 });
  }
  const sd = parseDate(startDate);
  const ed = parseDate(endDate);
  if (ed < sd) throw Object.assign(new Error('endDate 不能早于 startDate'), { statusCode: 400 });

  const startStr = `${sd.getFullYear()}-${pad(sd.getMonth() + 1)}-${pad(sd.getDate())} 00:00:00`;
  const endStr = `${ed.getFullYear()}-${pad(ed.getMonth() + 1)}-${pad(ed.getDate())} 23:59:59.999`;

  const pool = getPool();
  const where = [];
  const params = [];
  where.push('s.exit_time BETWEEN ? AND ?');
  params.push(startStr, endStr);
  where.push("s.status = 'FINISHED'");

  if (lotId !== undefined) {
    where.push('s.lot_id = ?');
    params.push(Number(lotId));
  }
  if (district) {
    where.push('l.district = ?');
    params.push(district);
  }

  const timeExpr = timeBucketExpr('s.exit_time', granularity);

  const whereClause = `WHERE ${where.join(' AND ')}`;

  /* 1) 按时段聚合：营收、车次、平均时长 —— 一把出 */
  const [trendRows] = await pool.query(
    `SELECT ${timeExpr} AS time_bucket,
            COUNT(*) AS session_count,
            SUM(s.fee_cents) AS revenue_cents,
            SUM(TIMESTAMPDIFF(SECOND, s.enter_time, s.exit_time)) AS duration_sec_sum
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     ${whereClause}
     GROUP BY time_bucket
     ORDER BY time_bucket`,
    params,
  );

  /* 2) 分渠道聚合 */
  const [chRows] = await pool.query(
    `SELECT ${timeExpr} AS time_bucket,
            s.payment_channel,
            SUM(s.fee_cents) AS revenue_cents,
            COUNT(*) AS session_count
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     ${whereClause}
     GROUP BY time_bucket, s.payment_channel
     ORDER BY time_bucket, s.payment_channel`,
    params,
  );

  /* 3) 分车型聚合 */
  const [vtRows] = await pool.query(
    `SELECT ${timeExpr} AS time_bucket,
            COALESCE(v.vehicle_type, 'UNKNOWN') AS vehicle_type,
            SUM(s.fee_cents) AS revenue_cents,
            COUNT(*) AS session_count
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     LEFT JOIN vehicles v ON v.plate_no = s.plate_no
     ${whereClause}
     GROUP BY time_bucket, vehicle_type
     ORDER BY time_bucket, vehicle_type`,
    params,
  );

  /* 4) 若按停车场维度，按 lot_id + 时段聚合 */
  let byLotRows = [];
  if (lotId === undefined && !district) {
    const [rows] = await pool.query(
      `SELECT s.lot_id, l.name AS lot_name, l.district,
              ${timeExpr} AS time_bucket,
              COUNT(*) AS session_count,
              SUM(s.fee_cents) AS revenue_cents
       FROM parking_sessions s
       LEFT JOIN parking_lots l ON l.id = s.lot_id
       ${whereClause}
       GROUP BY s.lot_id, time_bucket
       ORDER BY s.lot_id, time_bucket`,
      params,
    );
    byLotRows = rows;
  }

  /* 5) 若按区域维度，按 district + 时段聚合 */
  let byDistrictRows = [];
  if (!district && lotId === undefined) {
    const [rows] = await pool.query(
      `SELECT l.district,
              ${timeExpr} AS time_bucket,
              COUNT(*) AS session_count,
              SUM(s.fee_cents) AS revenue_cents
       FROM parking_sessions s
       LEFT JOIN parking_lots l ON l.id = s.lot_id
       ${whereClause}
       GROUP BY l.district, time_bucket
       ORDER BY l.district, time_bucket`,
      params,
    );
    byDistrictRows = rows;
  }

  /* 组装：按时间桶为 key，把各维度填进去 */
  const trendMap = new Map();
  for (const r of trendRows) {
    const sc = Number(r.session_count) || 0;
    const dur = Number(r.duration_sec_sum) || 0;
    const rev = Number(r.revenue_cents) || 0;
    trendMap.set(String(r.time_bucket), {
      timeBucket: String(r.time_bucket),
      sessionCount: sc,
      revenueCents: rev,
      revenueYuan: Number((rev / 100).toFixed(2)),
      avgDurationMinutes: sc > 0 ? Number(((dur / sc) / 60).toFixed(2)) : 0,
      byChannel: {},
      byVehicleType: {},
    });
  }

  /* 用空桶补全缺失的时间点（避免前端渲染断裂） */
  const allBuckets = generateTimeBuckets(sd, ed, granularity);
  for (const b of allBuckets) {
    if (!trendMap.has(b)) {
      trendMap.set(b, {
        timeBucket: b,
        sessionCount: 0,
        revenueCents: 0,
        revenueYuan: 0,
        avgDurationMinutes: 0,
        byChannel: {},
        byVehicleType: {},
      });
    }
  }

  for (const r of chRows) {
    const key = String(r.time_bucket);
    const node = trendMap.get(key);
    if (node) node.byChannel[r.payment_channel] = Number(r.revenue_cents) || 0;
  }
  for (const r of vtRows) {
    const key = String(r.time_bucket);
    const node = trendMap.get(key);
    if (node) node.byVehicleType[r.vehicle_type || 'UNKNOWN'] = Number(r.revenue_cents) || 0;
  }

  const trend = [...trendMap.values()].sort((a, b) => a.timeBucket.localeCompare(b.timeBucket));

  const byLot = byLotRows.map((r) => ({
    lotId: r.lot_id,
    lotName: r.lot_name,
    district: r.district,
    timeBucket: String(r.time_bucket),
    sessionCount: Number(r.session_count) || 0,
    revenueCents: Number(r.revenue_cents) || 0,
  }));

  const byDistrict = byDistrictRows.map((r) => ({
    district: r.district,
    timeBucket: String(r.time_bucket),
    sessionCount: Number(r.session_count) || 0,
    revenueCents: Number(r.revenue_cents) || 0,
  }));

  return {
    range: { start: startStr, end: endStr },
    granularity,
    filter: {
      lotId: lotId !== undefined ? Number(lotId) : null,
      district: district || null,
    },
    trend,
    byLot,
    byDistrict,
  };
}

function timeBucketExpr(col, granularity) {
  switch (granularity) {
    case 'hour':
      return `DATE_FORMAT(${col}, '%Y-%m-%d %H:00:00')`;
    case 'day':
      return `DATE_FORMAT(${col}, '%Y-%m-%d')`;
    case 'week':
      return `DATE_FORMAT(DATE_SUB(${col}, INTERVAL WEEKDAY(${col}) DAY), '%Y-%m-%d')`;
    default:
      throw new Error(`unknown granularity ${granularity}`);
  }
}

function generateTimeBuckets(start, end, granularity) {
  const buckets = [];
  const cur = new Date(start);
  const last = new Date(end);
  if (granularity === 'hour') {
    cur.setMinutes(0, 0, 0);
    last.setHours(23, 59, 59, 999);
    while (cur <= last) {
      const y = cur.getFullYear();
      const m = pad(cur.getMonth() + 1);
      const d = pad(cur.getDate());
      const h = pad(cur.getHours());
      buckets.push(`${y}-${m}-${d} ${h}:00:00`);
      cur.setHours(cur.getHours() + 1);
    }
  } else if (granularity === 'day') {
    cur.setHours(0, 0, 0, 0);
    while (cur <= last) {
      buckets.push(`${cur.getFullYear()}-${pad(cur.getMonth() + 1)}-${pad(cur.getDate())}`);
      cur.setDate(cur.getDate() + 1);
    }
  } else if (granularity === 'week') {
    cur.setHours(0, 0, 0, 0);
    const weekday = cur.getDay() === 0 ? 6 : cur.getDay() - 1;
    cur.setDate(cur.getDate() - weekday);
    while (cur <= last) {
      buckets.push(`${cur.getFullYear()}-${pad(cur.getMonth() + 1)}-${pad(cur.getDate())}`);
      cur.setDate(cur.getDate() + 7);
    }
  }
  return buckets;
}

/* ============================ 实时在场明细（用于对账） ============================ */

/**
 * 实时在场车辆明细（分页），用于和总览数字对账，确保汇总和明细对得上。
 */
async function listRealtimeSessions({ lotId, district, page = 1, pageSize = 50 } = {}) {
  const pool = getPool();
  const where = ["s.status = 'PARKED'"];
  const params = [];
  if (lotId !== undefined) { where.push('s.lot_id = ?'); params.push(Number(lotId)); }
  if (district) { where.push('l.district = ?'); params.push(district); }
  const clause = `WHERE ${where.join(' AND ')}`;

  const [[countRow]] = await pool.query(
    `SELECT COUNT(*) AS n FROM parking_sessions s LEFT JOIN parking_lots l ON l.id = s.lot_id ${clause}`,
    params,
  );
  const total = Number(countRow.n);

  const p = Math.max(1, Number(page) || 1);
  const ps = Math.max(1, Math.min(500, Number(pageSize) || 50));
  const offset = (p - 1) * ps;

  const [rows] = await pool.query(
    `SELECT s.*, l.name AS lot_name, l.district,
            COALESCE(v.vehicle_type, 'UNKNOWN') AS vehicle_type,
            COALESCE(v.owner_name, '') AS owner_name,
            COALESCE(v.phone, '') AS phone,
            TIMESTAMPDIFF(MINUTE, s.enter_time, NOW(3)) AS parked_minutes
     FROM parking_sessions s
     LEFT JOIN parking_lots l ON l.id = s.lot_id
     LEFT JOIN vehicles v ON v.plate_no = s.plate_no
     ${clause}
     ORDER BY s.enter_time DESC
     LIMIT ? OFFSET ?`,
    [...params, ps, offset],
  );

  return {
    total,
    page: p,
    pageSize: ps,
    totalPages: Math.ceil(total / ps),
    items: rows.map((r) => ({
      id: r.id,
      lotId: r.lot_id,
      lotName: r.lot_name,
      district: r.district,
      plateNo: r.plate_no,
      vehicleType: r.vehicle_type,
      ownerName: r.owner_name,
      phone: r.phone,
      enterTime: r.enter_time,
      parkedMinutes: Number(r.parked_minutes) || 0,
    })),
  };
}

module.exports = {
  getOverview,
  getDrilldown,
  listRealtimeSessions,
  todayRange,
  parseDate,
  timeBucketExpr,
  generateTimeBuckets,
};
