/**
 * Base de Datos MySQL para almacenar datos de Xinje HMI
 * Sistema modular con dispositivos, sensores e histórico con particionamiento
 */

const mysql = require('mysql2/promise');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

// Configuración de conexión a MySQL (XAMPP)
const dbConfig = {
  host: 'localhost',
  user: 'root',
  password: 'admin12345',  // XAMPP por defecto no tiene password
  database: 'xinje_iot',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// Debug: mostrar configuracion
console.log('[DB] Configuracion de conexion:');
console.log(`[DB] Host: ${dbConfig.host}`);
console.log(`[DB] Usuario: ${dbConfig.user}`);
console.log(`[DB] Base de datos: ${dbConfig.database}`);

let pool;

/**
 * Inicializar pool de conexiones
 */
async function initConnectionPool() {
  try {
    pool = await mysql.createPool(dbConfig);
    console.log('[DB] Conectado a MySQL');
    return pool;
  } catch (error) {
    console.error('[ERROR] Fallo al conectarse a MySQL:', error.message);
    throw error;
  }
}

/**
 * Inicializar esquema de base de datos MySQL
 */
async function initDatabase() {
  try {
    const connection = await pool.getConnection();
    
    // Tabla de dispositivos
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS devices (
        device_id INT AUTO_INCREMENT PRIMARY KEY,
        device_name VARCHAR(100) NOT NULL UNIQUE,
        device_type ENUM('PLC','HMI','PanelView') NOT NULL,
        location VARCHAR(100),
        description VARCHAR(255),
        status ENUM('online','offline') DEFAULT 'offline',
        last_seen DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_device_name (device_name),
        INDEX idx_last_seen (last_seen)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    // Tabla de sensores/tags
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensors (
        sensor_id INT AUTO_INCREMENT PRIMARY KEY,
        device_id INT NOT NULL,
        tag_name VARCHAR(100) NOT NULL,
        data_type ENUM('BOOL','INT','FLOAT','DOUBLE','STRING') NOT NULL DEFAULT 'DOUBLE',
        engineering_unit VARCHAR(20),
        description VARCHAR(255),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_device_tag (device_id, tag_name),
        FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE,
        INDEX idx_tag_name (tag_name)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    // Tabla de hist\u00f3rico de sensores con particionamiento por mes
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensor_history (
        sensor_id INT NOT NULL,
        timestamp DATETIME(3) NOT NULL,
        value DOUBLE,
        quality TINYINT DEFAULT 0,
        PRIMARY KEY(sensor_id, timestamp),
        FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id) ON DELETE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    // Tabla de hist\u00f3rico agregado por minuto (opcional pero recomendado)
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensor_history_1min (
        sensor_id INT NOT NULL,
        minute_ts DATETIME NOT NULL,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        count INT DEFAULT 0,
        PRIMARY KEY(sensor_id, minute_ts),
        FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        INDEX idx_timestamp (minute_ts)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    // Tabla de histórico agregado por 5 minutos
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensor_history_5min (
        sensor_id INT NOT NULL,
        interval_ts DATETIME NOT NULL,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        count INT DEFAULT 0,
        PRIMARY KEY(sensor_id, interval_ts),
        FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        INDEX idx_timestamp (interval_ts)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    // Tabla de histórico agregado por 10 minutos
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensor_history_10min (
        sensor_id INT NOT NULL,
        interval_ts DATETIME NOT NULL,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        count INT DEFAULT 0,
        PRIMARY KEY(sensor_id, interval_ts),
        FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        INDEX idx_timestamp (interval_ts)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    // Tabla de histórico agregado por 1 hora
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS sensor_history_1hour (
        sensor_id INT NOT NULL,
        hour_ts DATETIME NOT NULL,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        count INT DEFAULT 0,
        PRIMARY KEY(sensor_id, hour_ts),
        FOREIGN KEY(sensor_id) REFERENCES sensors(sensor_id) ON DELETE CASCADE,
        INDEX idx_timestamp (hour_ts)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);
    // Tabla de histórico de comandos
    await connection.execute(`
      CREATE TABLE IF NOT EXISTS command_history (
        command_id INT AUTO_INCREMENT PRIMARY KEY,
        device_id INT NOT NULL,
        command_json JSON NOT NULL,
        status ENUM('sent','acknowledged','failed') DEFAULT 'sent',
        timestamp DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE,
        INDEX idx_device_timestamp (device_id, timestamp)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    connection.release();
    console.log('[DB] Esquema de base de datos inicializado');
  } catch (error) {
    console.error('[ERROR] Error inicializando esquema:', error.message);
    throw error;
  }
}

/**
 * Guardar/actualizar dispositivo
 */
async function saveOrUpdateDevice(deviceId, deviceName, deviceType = 'HMI', location = null, description = null) {
  try {
    const connection = await pool.getConnection();
    
    await connection.execute(`
      INSERT INTO devices (device_name, device_type, location, description, status)
      VALUES (?, ?, ?, ?, 'online')
      ON DUPLICATE KEY UPDATE
        status = 'online',
        last_seen = CURRENT_TIMESTAMP
    `, [deviceName, deviceType, location, description]);
    
    connection.release();
    
    // Obtener el device_id
    const [rows] = await pool.execute(
      'SELECT device_id FROM devices WHERE device_name = ?',
      [deviceName]
    );
    
    return rows[0]?.device_id;
  } catch (error) {
    console.error('[ERROR] Error guardando dispositivo:', error.message);
    throw error;
  }
}

/**
 * Obtener o crear sensor/tag
 */
async function getOrCreateSensor(deviceId, tagName, dataType = 'DOUBLE', unit = null, description = null) {
  try {
    const connection = await pool.getConnection();
    
    // Verificar si el sensor existe
    const [existing] = await connection.execute(
      'SELECT sensor_id FROM sensors WHERE device_id = ? AND tag_name = ?',
      [deviceId, tagName]
    );
    
    if (existing.length > 0) {
      connection.release();
      return existing[0].sensor_id;
    }
    
    // Crear nuevo sensor
    const [result] = await connection.execute(`
      INSERT INTO sensors (device_id, tag_name, data_type, engineering_unit, description)
      VALUES (?, ?, ?, ?, ?)
    `, [deviceId, tagName, dataType, unit, description]);
    
    connection.release();
    return result.insertId;
  } catch (error) {
    console.error('[ERROR] Error creando sensor:', error.message);
    throw error;
  }
}

/**
 * Guardar datos de sensores (nueva estructura)
 * NOTA: Usa UTC_TIMESTAMP(3) para garantizar compatibilidad con eventos en cualquier zona horaria
 */
async function saveData(data) {
  try {
    // 1. Obtener o crear dispositivo
    const deviceId = await saveOrUpdateDevice(
      data.deviceId,
      String(data.deviceId),
      'HMI'
    );
    
    if (!deviceId) {
      throw new Error('No se pudo crear/obtener el dispositivo');
    }
    
    // 2. Determinar variables a procesar
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
    
    const connection = await pool.getConnection();
    
    for (const variable of varsToProcess) {
      // 3. Obtener o crear sensor
      const sensorId = await getOrCreateSensor(
        deviceId,
        variable.tag_name || variable.key || variable.name,
        'DOUBLE',
        null,
        null
      );
      
      // 4. Insertar histórico usando UTC_TIMESTAMP(3) de MySQL
      // Esto garantiza que los datos siempre estén en UTC, independiente de la zona horaria
      const value = parseFloat(variable.value) || 0;
      
      await connection.execute(`
        INSERT INTO sensor_history (sensor_id, timestamp, value, quality)
        VALUES (?, UTC_TIMESTAMP(3), ?, 1)
      `, [sensorId, value]);
    }
    
    connection.release();
  } catch (error) {
    console.error('[ERROR] Error guardando datos:', error.message);
  }
}

/**
 * Guardar configuración del dispositivo (JSON)
 */
async function saveConfig(deviceId, config) {
  try {
    const connection = await pool.getConnection();
    
    // Obtener el device_id numérico
    const [devices] = await connection.execute(
      'SELECT device_id FROM devices WHERE device_id = ? LIMIT 1',
      [deviceId]
    );
    
    if (devices.length === 0) {
      // Si el device_id numérico no existe, usar el device_name
      await saveOrUpdateDevice(deviceId, String(deviceId), 'HMI');
    }
    
    connection.release();
  } catch (error) {
    console.error('[ERROR] Error guardando configuración:', error.message);
  }
}

/**
 * Obtener últimos datos de un dispositivo
 */
async function getLatestData(deviceId, limit = 100) {
  try {
    const [rows] = await pool.execute(`
      SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality
      FROM sensor_history sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY sh.timestamp DESC
      LIMIT ?
    `, [deviceId, deviceId, limit]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo últimos datos:', error.message);
    return [];
  }
}

/**
 * Obtener últimos valores únicos por tag/sensor
 */
async function getLatestByKey(deviceId) {
  try {
    const [rows] = await pool.execute(`
      SELECT 
        s.sensor_id,
        s.tag_name,
        sh.timestamp,
        sh.value,
        sh.quality
      FROM sensors s
      LEFT JOIN sensor_history sh ON s.sensor_id = sh.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      AND sh.timestamp = (
        SELECT MAX(timestamp) FROM sensor_history 
        WHERE sensor_id = s.sensor_id
      )
      ORDER BY s.tag_name ASC
    `, [deviceId, deviceId]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo últimos valores por clave:', error.message);
    return [];
  }
}

/**
 * Obtener datos históricos con filtros
 */
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
      query += ` AND s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )`;
      params.push(options.deviceId, options.deviceId);
    }

    if (options.tag) {
      query += ' AND s.tag_name = ?';
      params.push(options.tag);
    }

    if (options.startTime) {
      query += ' AND sh.timestamp >= ?';
      params.push(new Date(options.startTime).toISOString());
    }

    if (options.endTime) {
      query += ' AND sh.timestamp <= ?';
      params.push(new Date(options.endTime).toISOString());
    }

    query += ' ORDER BY sh.timestamp DESC';

    if (options.limit) {
      query += ' LIMIT ?';
      params.push(options.limit);
    }

    const [rows] = await pool.execute(query, params);
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo datos históricos:', error.message);
    return [];
  }
}

/**
 * Obtener datos con paginación (OPTIMIZADO para MySQL)
 */
async function getHistoricalDataPaginated(options = {}) {
  try {
    const page = Math.max(0, parseInt(options.page) || 0);
    const pageSize = Math.min(500, Math.max(1, parseInt(options.pageSize) || 50));
    
    let query = `
      SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality
      FROM sensor_history sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE 1=1
    `;
    const params = [];

    if (options.deviceId) {
      query += ` AND s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )`;
      params.push(options.deviceId, options.deviceId);
    }

    if (options.tag) {
      query += ' AND s.tag_name = ?';
      params.push(options.tag);
    }

    if (options.startTime) {
      query += ' AND sh.timestamp >= ?';
      params.push(new Date(options.startTime).toISOString());
    }

    if (options.endTime) {
      query += ' AND sh.timestamp <= ?';
      params.push(new Date(options.endTime).toISOString());
    }

    // Conteo total
    const countQuery = query.replace(
      'SELECT sh.sensor_id, s.tag_name, sh.timestamp, sh.value, sh.quality',
      'SELECT COUNT(*) as total'
    );
    const [countResult] = await pool.execute(countQuery, params);
    const total = countResult[0]?.total || 0;

    // Paginación
    const offset = page * pageSize;
    query += ' ORDER BY sh.timestamp DESC LIMIT ? OFFSET ?';
    params.push(pageSize, offset);

    const [data] = await pool.execute(query, params);

    return {
      data,
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
    return { data: [], pagination: { page: 0, pageSize: 50, total: 0, totalPages: 0, hasNext: false, hasPrev: false } };
  }
}

/**
 * Obtener lista de dispositivos
 */
async function getDevices() {
  try {
    const [rows] = await pool.execute(`
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

/**
 * Obtener sensores de un dispositivo
 */
async function getSensors(deviceId) {
  try {
    const [rows] = await pool.execute(`
      SELECT * FROM sensors
      WHERE device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY tag_name ASC
    `, [deviceId, deviceId]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo sensores:', error.message);
    return [];
  }
}

/**
 * Guardar comando enviado
 */
async function saveCommand(deviceId, command) {
  try {
    const connection = await pool.getConnection();
    
    await connection.execute(`
      INSERT INTO command_history (device_id, command_json, status)
      VALUES (?, ?, 'sent')
    `, [deviceId, JSON.stringify(command)]);
    
    connection.release();
  } catch (error) {
    console.error('[ERROR] Error guardando comando:', error.message);
  }
}

/**
 * Obtener estadísticas de la base de datos
 */
async function getStats() {
  try {
    const [stats] = await pool.execute(`
      SELECT 
        (SELECT COUNT(*) FROM sensor_history) as totalRecords,
        (SELECT COUNT(*) FROM devices) as totalDevices,
        ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) as databaseSizeMB
      FROM information_schema.TABLES 
      WHERE table_schema = DATABASE()
    `);
    
    return {
      totalRecords: stats[0]?.totalRecords || 0,
      totalDevices: stats[0]?.totalDevices || 0,
      databaseSizeMB: stats[0]?.databaseSizeMB || 0,
      database: dbConfig.database
    };
  } catch (error) {
    console.error('[ERROR] Error obteniendo estadísticas:', error.message);
    return { totalRecords: 0, totalDevices: 0, databaseSizeMB: 0 };
  }
}

/**
 * Limpiar datos antiguos (mantenimiento)
 */
async function cleanOldData(daysToKeep = 30) {
  try {
    const connection = await pool.getConnection();
    
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
    
    const [result] = await connection.execute(
      'DELETE FROM sensor_history WHERE timestamp < ?',
      [cutoffDate.toISOString()]
    );
    
    console.log(`[DB] Limpieza: ${result.affectedRows} registros antiguos eliminados`);
    
    connection.release();
    return result.affectedRows;
  } catch (error) {
    console.error('[ERROR] Error limpiando datos antiguos:', error.message);
    return 0;
  }
}

/**
 * Obtener datos agregados por 1 minuto
 */
async function getAggregated1min(deviceId, limit = 100) {
  try {
    const [rows] = await pool.execute(`
      SELECT sh.sensor_id, s.tag_name, sh.minute_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
      FROM sensor_history_1min sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY sh.minute_ts DESC
      LIMIT ?
    `, [deviceId, deviceId, limit]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 1min:', error.message);
    return [];
  }
}

/**
 * Obtener datos agregados por 5 minutos
 */
async function getAggregated5min(deviceId, limit = 100) {
  try {
    const [rows] = await pool.execute(`
      SELECT sh.sensor_id, s.tag_name, sh.interval_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
      FROM sensor_history_5min sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY sh.interval_ts DESC
      LIMIT ?
    `, [deviceId, deviceId, limit]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 5min:', error.message);
    return [];
  }
}

/**
 * Obtener datos agregados por 10 minutos
 */
async function getAggregated10min(deviceId, limit = 100) {
  try {
    const [rows] = await pool.execute(`
      SELECT sh.sensor_id, s.tag_name, sh.interval_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
      FROM sensor_history_10min sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY sh.interval_ts DESC
      LIMIT ?
    `, [deviceId, deviceId, limit]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 10min:', error.message);
    return [];
  }
}

/**
 * Obtener datos agregados por 1 hora
 */
async function getAggregated1hour(deviceId, limit = 24) {
  try {
    const [rows] = await pool.execute(`
      SELECT sh.sensor_id, s.tag_name, sh.hour_ts, sh.avg_value, sh.min_value, sh.max_value, sh.count
      FROM sensor_history_1hour sh
      JOIN sensors s ON sh.sensor_id = s.sensor_id
      WHERE s.device_id = (
        SELECT device_id FROM devices WHERE device_id = ? OR device_name = ? LIMIT 1
      )
      ORDER BY sh.hour_ts DESC
      LIMIT ?
    `, [deviceId, deviceId, limit]);
    
    return rows;
  } catch (error) {
    console.error('[ERROR] Error obteniendo agregados 1hour:', error.message);
    return [];
  }
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
  getAggregated1hour
};
