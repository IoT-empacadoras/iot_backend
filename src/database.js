/**
 * Base de Datos PostgreSQL para almacenar datos de Xinje HMI
 * Sistema modular con dispositivos, sensores e histórico
 */

const { Pool } = require('pg');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const sslEnabled = process.env.DB_SSL === 'true';
const rejectUnauthorized = process.env.DB_SSL_REJECT_UNAUTHORIZED !== 'false';

const dbConfig = process.env.DATABASE_URL
  ? {
      connectionString: process.env.DATABASE_URL,
      max: parseInt(process.env.DB_POOL_MAX || '10', 10),
      ssl: sslEnabled ? { rejectUnauthorized } : false
    }
  : {
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      database: process.env.DB_NAME || 'xinje_iot',
      max: parseInt(process.env.DB_POOL_MAX || '10', 10),
      ssl: sslEnabled ? { rejectUnauthorized } : false
    };

console.log('[DB] Configuracion de conexion:');
console.log(`[DB] Host: ${process.env.DATABASE_URL ? 'via DATABASE_URL' : dbConfig.host}`);
console.log(`[DB] Puerto: ${process.env.DATABASE_URL ? 'n/a' : dbConfig.port}`);
console.log(`[DB] Usuario: ${process.env.DATABASE_URL ? 'n/a' : dbConfig.user}`);
console.log(`[DB] Base de datos: ${process.env.DATABASE_URL ? 'definida en DATABASE_URL' : dbConfig.database}`);
console.log(`[DB] SSL: ${sslEnabled ? 'habilitado' : 'deshabilitado'}`);

let pool;
let aggregationTimers = [];

async function initConnectionPool() {
  try {
    pool = new Pool(dbConfig);
    await pool.query('SELECT 1');
    console.log('[DB] Conectado a PostgreSQL');
    return pool;
  } catch (error) {
    console.error('[ERROR] Fallo al conectarse a PostgreSQL:', error.message);
    throw error;
  }
}

async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS devices (
        device_id SERIAL PRIMARY KEY,
        device_name VARCHAR(100) NOT NULL UNIQUE,
        device_type VARCHAR(20) NOT NULL CHECK (device_type IN ('PLC','HMI','PanelView')),
        location VARCHAR(100),
        description VARCHAR(255),
        status VARCHAR(20) DEFAULT 'offline' CHECK (status IN ('online','offline')),
        last_seen TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query('CREATE INDEX IF NOT EXISTS idx_device_name ON devices (device_name)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_last_seen ON devices (last_seen)');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensors (
        sensor_id SERIAL PRIMARY KEY,
        device_id INT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
        tag_name VARCHAR(100) NOT NULL,
        data_type VARCHAR(20) NOT NULL DEFAULT 'DOUBLE' CHECK (data_type IN ('BOOL','INT','FLOAT','DOUBLE','STRING')),
        engineering_unit VARCHAR(20),
        description VARCHAR(255),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (device_id, tag_name)
      )
    `);

    await pool.query('CREATE INDEX IF NOT EXISTS idx_tag_name ON sensors (tag_name)');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_history (
        sensor_id INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        value DOUBLE PRECISION,
        quality SMALLINT DEFAULT 0,
        PRIMARY KEY (sensor_id, timestamp)
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_history_1min (
        sensor_id INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        minute_ts TIMESTAMPTZ NOT NULL,
        avg_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        count INT DEFAULT 0,
        PRIMARY KEY (sensor_id, minute_ts)
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_history_5min (
        sensor_id INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        interval_ts TIMESTAMPTZ NOT NULL,
        avg_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        count INT DEFAULT 0,
        PRIMARY KEY (sensor_id, interval_ts)
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_history_10min (
        sensor_id INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        interval_ts TIMESTAMPTZ NOT NULL,
        avg_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        count INT DEFAULT 0,
        PRIMARY KEY (sensor_id, interval_ts)
      )
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sensor_history_1hour (
        sensor_id INT NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        hour_ts TIMESTAMPTZ NOT NULL,
        avg_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        count INT DEFAULT 0,
        PRIMARY KEY (sensor_id, hour_ts)
      )
    `);

    await pool.query('CREATE INDEX IF NOT EXISTS idx_1min_ts ON sensor_history_1min (minute_ts)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_5min_ts ON sensor_history_5min (interval_ts)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_10min_ts ON sensor_history_10min (interval_ts)');
    await pool.query('CREATE INDEX IF NOT EXISTS idx_1hour_ts ON sensor_history_1hour (hour_ts)');

    await pool.query(`
      CREATE TABLE IF NOT EXISTS command_history (
        command_id SERIAL PRIMARY KEY,
        device_id INT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
        command_json JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'sent' CHECK (status IN ('sent','acknowledged','failed')),
        timestamp TIMESTAMPTZ DEFAULT NOW(),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await pool.query('CREATE INDEX IF NOT EXISTS idx_device_timestamp ON command_history (device_id, timestamp)');

    console.log('[DB] Esquema de base de datos inicializado');
  } catch (error) {
    console.error('[ERROR] Error inicializando esquema:', error.message);
    throw error;
  }
}

async function resolveDeviceId(deviceRef) {
  const ref = String(deviceRef);
  const { rows } = await pool.query(
    `SELECT device_id FROM devices WHERE device_name = $1 OR device_id::text = $1 LIMIT 1`,
    [ref]
  );
  return rows[0]?.device_id || null;
}

async function saveOrUpdateDevice(deviceId, deviceName, deviceType = 'HMI', location = null, description = null) {
  try {
    const { rows } = await pool.query(
      `
        INSERT INTO devices (device_name, device_type, location, description, status, last_seen)
        VALUES ($1, $2, $3, $4, 'online', NOW())
        ON CONFLICT (device_name)
        DO UPDATE SET
          status = 'online',
          last_seen = NOW(),
          location = COALESCE(EXCLUDED.location, devices.location),
          description = COALESCE(EXCLUDED.description, devices.description)
        RETURNING device_id
      `,
      [deviceName, deviceType, location, description]
    );

    return rows[0]?.device_id;
  } catch (error) {
    console.error('[ERROR] Error guardando dispositivo:', error.message);
    throw error;
  }
}

async function getOrCreateSensor(deviceId, tagName, dataType = 'DOUBLE', unit = null, description = null) {
  try {
    const { rows: existing } = await pool.query(
      'SELECT sensor_id FROM sensors WHERE device_id = $1 AND tag_name = $2',
      [deviceId, tagName]
    );

    if (existing.length > 0) {
      return existing[0].sensor_id;
    }

    const { rows } = await pool.query(
      `
        INSERT INTO sensors (device_id, tag_name, data_type, engineering_unit, description)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (device_id, tag_name)
        DO UPDATE SET data_type = EXCLUDED.data_type
        RETURNING sensor_id
      `,
      [deviceId, tagName, dataType, unit, description]
    );

    return rows[0].sensor_id;
  } catch (error) {
    console.error('[ERROR] Error creando sensor:', error.message);
    throw error;
  }
}

async function saveData(data) {
  try {
    const deviceId = await saveOrUpdateDevice(
      data.deviceId,
      String(data.deviceId),
      'HMI'
    );

    if (!deviceId) {
      throw new Error('No se pudo crear/obtener el dispositivo');
    }

    let varsToProcess = [];
    if (Array.isArray(data.variables)) {
      varsToProcess = data.variables;
    } else if (data.variables && typeof data.variables === 'object') {
      varsToProcess = Object.keys(data.variables).map((key) => ({
        tag_name: key,
        value: data.variables[key]
      }));
    } else if (data.key !== undefined) {
      varsToProcess = [{
        tag_name: data.key,
        value: data.value
      }];
    }

    for (const variable of varsToProcess) {
      const sensorId = await getOrCreateSensor(
        deviceId,
        variable.tag_name || variable.key || variable.name,
        'DOUBLE',
        null,
        null
      );

      const value = parseFloat(variable.value) || 0;

      await pool.query(
        `
          INSERT INTO sensor_history (sensor_id, timestamp, value, quality)
          VALUES ($1, NOW(), $2, 1)
        `,
        [sensorId, value]
      );
    }
  } catch (error) {
    console.error('[ERROR] Error guardando datos:', error.message);
  }
}

async function saveConfig(deviceId, config) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);

    if (!resolvedId) {
      await saveOrUpdateDevice(deviceId, String(deviceId), 'HMI');
    }
  } catch (error) {
    console.error('[ERROR] Error guardando configuración:', error.message);
  }
}

async function getLatestData(deviceId, limit = 100) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality
        FROM sensor_history sh
        JOIN sensors s ON sh.sensor_id = s.sensor_id
        WHERE s.device_id = $1
        ORDER BY sh.timestamp DESC
        LIMIT $2
      `,
      [resolvedId, limit]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo últimos datos:', error.message);
    return [];
  }
}

async function getLatestByKey(deviceId) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT
          s.sensor_id,
          s.tag_name,
          sh.timestamp,
          sh.value,
          sh.quality
        FROM sensors s
        JOIN LATERAL (
          SELECT timestamp, value, quality
          FROM sensor_history
          WHERE sensor_id = s.sensor_id
          ORDER BY timestamp DESC
          LIMIT 1
        ) sh ON true
        WHERE s.device_id = $1
        ORDER BY s.tag_name ASC
      `,
      [resolvedId]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo últimos valores por clave:', error.message);
    return [];
  }
}

async function getHistoricalData(options = {}) {
  try {
    let query = `
      SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality
      FROM sensor_history sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE 1=1
    `;

    const params = [];

    if (options.deviceId) {
      const resolvedId = await resolveDeviceId(options.deviceId);
      if (!resolvedId) return [];
      params.push(resolvedId);
      query += ` AND s.device_id = $${params.length}`;
    }

    const selectedTag = options.tag || options.key;
    if (selectedTag) {
      params.push(selectedTag);
      query += ` AND s.tag_name = $${params.length}`;
    }

    if (options.startTime) {
      params.push(new Date(options.startTime).toISOString());
      query += ` AND sh.timestamp >= $${params.length}`;
    }

    if (options.endTime) {
      params.push(new Date(options.endTime).toISOString());
      query += ` AND sh.timestamp <= $${params.length}`;
    }

    query += ' ORDER BY sh.timestamp DESC';

    if (options.limit) {
      params.push(parseInt(options.limit, 10));
      query += ` LIMIT $${params.length}`;
    }

    const { rows } = await pool.query(query, params);
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo datos históricos:', error.message);
    return [];
  }
}

async function getHistoricalDataPaginated(options = {}) {
  try {
    const page = Math.max(0, parseInt(options.page, 10) || 0);
    const pageSize = Math.min(500, Math.max(1, parseInt(options.pageSize, 10) || 50));

    let baseQuery = `
      FROM sensor_history sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE 1=1
    `;

    const params = [];

    if (options.deviceId) {
      const resolvedId = await resolveDeviceId(options.deviceId);
      if (!resolvedId) {
        return {
          data: [],
          pagination: { page, pageSize, total: 0, totalPages: 0, hasNext: false, hasPrev: page > 0 }
        };
      }
      params.push(resolvedId);
      baseQuery += ` AND s.device_id = $${params.length}`;
    }

    const selectedTag = options.tag || options.key;
    if (selectedTag) {
      params.push(selectedTag);
      baseQuery += ` AND s.tag_name = $${params.length}`;
    }

    if (options.startTime) {
      params.push(new Date(options.startTime).toISOString());
      baseQuery += ` AND sh.timestamp >= $${params.length}`;
    }

    if (options.endTime) {
      params.push(new Date(options.endTime).toISOString());
      baseQuery += ` AND sh.timestamp <= $${params.length}`;
    }

    const countQuery = `SELECT COUNT(*)::int as total ${baseQuery}`;
    const countResult = await pool.query(countQuery, params);
    const total = countResult.rows[0]?.total || 0;

    const offset = page * pageSize;
    const dataParams = [...params, pageSize, offset];

    const dataQuery = `
      SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality
      ${baseQuery}
      ORDER BY sh.timestamp DESC
      LIMIT $${params.length + 1}
      OFFSET $${params.length + 2}
    `;

    const { rows } = await pool.query(dataQuery, dataParams);

    return {
      data: rows,
      pagination: {
        page,
        pageSize,
        total,
        totalPages: Math.ceil(total / pageSize),
        hasNext: (page + 1) * pageSize < total,
        hasPrev: page > 0
      }
    };
  } catch (error) {
    console.error('[ERROR] Error obteniendo datos paginados:', error.message);
    return {
      data: [],
      pagination: { page: 0, pageSize: 50, total: 0, totalPages: 0, hasNext: false, hasPrev: false }
    };
  }
}

async function getDevices() {
  try {
    const { rows } = await pool.query(`
      SELECT
        device_id,
        device_name,
        device_type,
        location,
        status,
        last_seen,
        created_at
      FROM devices
      ORDER BY last_seen DESC
    `);

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo dispositivos:', error.message);
    return [];
  }
}

async function getSensors(deviceId) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT * FROM sensors
        WHERE device_id = $1
        ORDER BY tag_name ASC
      `,
      [resolvedId]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo sensores:', error.message);
    return [];
  }
}

async function saveCommand(deviceId, command) {
  try {
    let resolvedId = await resolveDeviceId(deviceId);

    if (!resolvedId) {
      resolvedId = await saveOrUpdateDevice(deviceId, String(deviceId), 'HMI');
    }

    await pool.query(
      `
        INSERT INTO command_history (device_id, command_json, status)
        VALUES ($1, $2::jsonb, 'sent')
      `,
      [resolvedId, JSON.stringify(command)]
    );
  } catch (error) {
    console.error('[ERROR] Error guardando comando:', error.message);
  }
}

async function getStats() {
  try {
    const { rows } = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM sensor_history) as "totalRecords",
        (SELECT COUNT(*)::int FROM devices) as "totalDevices",
        ROUND((pg_database_size(current_database())::numeric / 1024 / 1024), 2) as "databaseSizeMB"
    `);

    return {
      totalRecords: rows[0]?.totalRecords || 0,
      totalDevices: rows[0]?.totalDevices || 0,
      databaseSizeMB: parseFloat(rows[0]?.databaseSizeMB || 0),
      database: process.env.DB_NAME || 'render-postgres'
    };
  } catch (error) {
    console.error('[ERROR] Error obteniendo estadísticas:', error.message);
    return { totalRecords: 0, totalDevices: 0, databaseSizeMB: 0 };
  }
}

async function cleanOldData(daysToKeep = 30) {
  try {
    const result = await pool.query(
      `DELETE FROM sensor_history WHERE timestamp < NOW() - ($1::int * INTERVAL '1 day')`,
      [daysToKeep]
    );

    console.log(`[DB] Limpieza: ${result.rowCount} registros antiguos eliminados`);
    return result.rowCount;
  } catch (error) {
    console.error('[ERROR] Error limpiando datos antiguos:', error.message);
    return 0;
  }
}

async function getAggregated1min(deviceId, limit = 100) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT sh.sensor_id, s.tag_name, sh.minute_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
        FROM sensor_history_1min sh
        JOIN sensors s ON sh.sensor_id = s.sensor_id
        WHERE s.device_id = $1
        ORDER BY sh.minute_ts DESC
        LIMIT $2
      `,
      [resolvedId, limit]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 1min:', error.message);
    return [];
  }
}

async function getAggregated5min(deviceId, limit = 100) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT sh.sensor_id, s.tag_name, sh.interval_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
        FROM sensor_history_5min sh
        JOIN sensors s ON sh.sensor_id = s.sensor_id
        WHERE s.device_id = $1
        ORDER BY sh.interval_ts DESC
        LIMIT $2
      `,
      [resolvedId, limit]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 5min:', error.message);
    return [];
  }
}

async function getAggregated10min(deviceId, limit = 100) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT sh.sensor_id, s.tag_name, sh.interval_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
        FROM sensor_history_10min sh
        JOIN sensors s ON sh.sensor_id = s.sensor_id
        WHERE s.device_id = $1
        ORDER BY sh.interval_ts DESC
        LIMIT $2
      `,
      [resolvedId, limit]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 10min:', error.message);
    return [];
  }
}

async function getAggregated1hour(deviceId, limit = 24) {
  try {
    const resolvedId = await resolveDeviceId(deviceId);
    if (!resolvedId) return [];

    const { rows } = await pool.query(
      `
        SELECT sh.sensor_id, s.tag_name, sh.hour_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
        FROM sensor_history_1hour sh
        JOIN sensors s ON sh.sensor_id = s.sensor_id
        WHERE s.device_id = $1
        ORDER BY sh.hour_ts DESC
        LIMIT $2
      `,
      [resolvedId, limit]
    );

    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 1hour:', error.message);
    return [];
  }
}

async function aggregate1min() {
  await pool.query(`
    INSERT INTO sensor_history_1min (sensor_id, minute_ts, avg_value, min_value, max_value, count)
    SELECT
      sensor_id,
      date_trunc('minute', "timestamp") AS minute_ts,
      AVG(value) AS avg_value,
      MIN(value) AS min_value,
      MAX(value) AS max_value,
      COUNT(*)::int AS count
    FROM sensor_history
    WHERE "timestamp" >= now() - INTERVAL '2 minute'
      AND "timestamp" < date_trunc('minute', now())
    GROUP BY sensor_id, date_trunc('minute', "timestamp")
    ON CONFLICT (sensor_id, minute_ts)
    DO UPDATE SET
      avg_value = EXCLUDED.avg_value,
      min_value = EXCLUDED.min_value,
      max_value = EXCLUDED.max_value,
      count = EXCLUDED.count
  `);
}

async function aggregate5min() {
  await pool.query(`
    INSERT INTO sensor_history_5min (sensor_id, interval_ts, avg_value, min_value, max_value, count)
    SELECT
      sensor_id,
      to_timestamp(floor(extract(epoch FROM "timestamp") / 300) * 300) AS interval_ts,
      AVG(value) AS avg_value,
      MIN(value) AS min_value,
      MAX(value) AS max_value,
      COUNT(*)::int AS count
    FROM sensor_history
    WHERE "timestamp" >= now() - INTERVAL '10 minute'
      AND "timestamp" < to_timestamp(floor(extract(epoch FROM now()) / 300) * 300)
    GROUP BY sensor_id, to_timestamp(floor(extract(epoch FROM "timestamp") / 300) * 300)
    ON CONFLICT (sensor_id, interval_ts)
    DO UPDATE SET
      avg_value = EXCLUDED.avg_value,
      min_value = EXCLUDED.min_value,
      max_value = EXCLUDED.max_value,
      count = EXCLUDED.count
  `);
}

async function aggregate10min() {
  await pool.query(`
    INSERT INTO sensor_history_10min (sensor_id, interval_ts, avg_value, min_value, max_value, count)
    SELECT
      sensor_id,
      to_timestamp(floor(extract(epoch FROM "timestamp") / 600) * 600) AS interval_ts,
      AVG(value) AS avg_value,
      MIN(value) AS min_value,
      MAX(value) AS max_value,
      COUNT(*)::int AS count
    FROM sensor_history
    WHERE "timestamp" >= now() - INTERVAL '20 minute'
      AND "timestamp" < to_timestamp(floor(extract(epoch FROM now()) / 600) * 600)
    GROUP BY sensor_id, to_timestamp(floor(extract(epoch FROM "timestamp") / 600) * 600)
    ON CONFLICT (sensor_id, interval_ts)
    DO UPDATE SET
      avg_value = EXCLUDED.avg_value,
      min_value = EXCLUDED.min_value,
      max_value = EXCLUDED.max_value,
      count = EXCLUDED.count
  `);
}

async function aggregate1hour() {
  await pool.query(`
    INSERT INTO sensor_history_1hour (sensor_id, hour_ts, avg_value, min_value, max_value, count)
    SELECT
      sensor_id,
      date_trunc('hour', "timestamp") AS hour_ts,
      AVG(value) AS avg_value,
      MIN(value) AS min_value,
      MAX(value) AS max_value,
      COUNT(*)::int AS count
    FROM sensor_history
    WHERE "timestamp" >= now() - INTERVAL '2 hour'
      AND "timestamp" < date_trunc('hour', now())
    GROUP BY sensor_id, date_trunc('hour', "timestamp")
    ON CONFLICT (sensor_id, hour_ts)
    DO UPDATE SET
      avg_value = EXCLUDED.avg_value,
      min_value = EXCLUDED.min_value,
      max_value = EXCLUDED.max_value,
      count = EXCLUDED.count
  `);
}

function createAggregationTimer(name, intervalMs, worker) {
  const run = async () => {
    try {
      await worker();
    } catch (error) {
      console.error(`[ERROR] Job ${name} falló:`, error.message);
    }
  };

  run();
  const timer = setInterval(run, intervalMs);
  aggregationTimers.push(timer);
  console.log(`[DB] Job ${name} activo cada ${Math.round(intervalMs / 1000)}s`);
}

function startAggregationJobs() {
  if (aggregationTimers.length > 0) {
    return;
  }

  createAggregationTimer('aggregate_1min', 60 * 1000, aggregate1min);
  createAggregationTimer('aggregate_5min', 5 * 60 * 1000, aggregate5min);
  createAggregationTimer('aggregate_10min', 10 * 60 * 1000, aggregate10min);
  createAggregationTimer('aggregate_1hour', 60 * 60 * 1000, aggregate1hour);
}

function stopAggregationJobs() {
  aggregationTimers.forEach((timer) => clearInterval(timer));
  aggregationTimers = [];
}

module.exports = {
  initConnectionPool,
  initDatabase,
  saveData,
  saveConfig,
  getLatestData,
  getLatestByKey,
  getHistoricalData,
  getHistoricalDataPaginated,
  getDevices,
  getSensors,
  saveCommand,
  getStats,
  cleanOldData,
  saveOrUpdateDevice,
  getOrCreateSensor,
  getAggregated1min,
  getAggregated5min,
  getAggregated10min,
  getAggregated1hour,
  startAggregationJobs,
  stopAggregationJobs
};
