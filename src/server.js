/**
 * Servidor Principal - Backend para Xinje TouchWin Pro
 * 
 * Este servidor integra:
 * - API REST para consultas y comandos
 * - WebSocket para datos en tiempo real
 * - Cliente MQTT para comunicación con HMI Xinje
 */

require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const cors = require('cors');
const XinjeMQTTHandler = require('./mqttHandler');
const { 
  initConnectionPool,
  initDatabase,
  getLatestData, 
  getLatestByKey, 
  getHistoricalData,
  getHistoricalDataPaginated, 
  getDevices,
  getSensors,
  setActiveSensors,
  activatePpmProfile,
  getStats,
  refreshDevicesOnlineStatus,
  saveCommand,
  saveOrUpdateDevice,
  getAggregated1min,
  getAggregated5min,
  getAggregated10min,
  getAggregated1hour,
  getMachineEvents,
  startAggregationJobs,
  stopAggregationJobs
} = require('./database');

// ========================================
// CONFIGURACIÓN
// ========================================

const PORT = process.env.PORT || 3000;
// Por defecto usar broker público de HiveMQ si no se define otro
const MQTT_BROKER = process.env.MQTT_BROKER || 'mqtt://broker.hivemq.com:1883';
const ENABLE_APP_AGGREGATION_JOBS = process.env.ENABLE_APP_AGGREGATION_JOBS !== 'false';
const DEVICE_STATUS_WINDOW_MINUTES = parseInt(process.env.DEVICE_STATUS_WINDOW_MINUTES || '5', 10);
const DEVICE_STATUS_SYNC_INTERVAL_MS = parseInt(process.env.DEVICE_STATUS_SYNC_INTERVAL_MS || '300000', 10);

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: process.env.CORS_ORIGIN || '*',
    methods: ['GET', 'POST']
  }
});

// ========================================
// MIDDLEWARE
// ========================================

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Logger middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

// ========================================
// MQTT HANDLER
// ========================================

const mqttHandler = new XinjeMQTTHandler(MQTT_BROKER, {
  clientId: process.env.MQTT_CLIENT_ID,
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD
});

// Suscribirse a eventos MQTT y reenviar por WebSocket
mqttHandler.subscribe((data) => {
  io.emit('xinje-data', data);
  
  // Actualizar estado del dispositivo en PostgreSQL
  const deviceName = String(data.deviceId);
  
  saveOrUpdateDevice(data.deviceId, deviceName, 'HMI').catch((error) => {
    console.error('[ERROR] Error actualizando dispositivo:', error.message);
  });
});

// ========================================
// API REST ENDPOINTS
// ========================================

/**
 * GET /api/health
 * Estado del servidor
 */
app.get('/api/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    mqtt: mqttHandler.client ? 'connected' : 'disconnected'
  });
});

/**
 * GET /
 * Información básica del servicio
 */
app.get('/', (req, res) => {
  res.json({
    service: 'Xinje IoT Backend',
    status: 'running',
    health: '/api/health'
  });
});

/**
 * GET /api/stats
 * Estadísticas de la base de datos
 */
app.get('/api/stats', async (req, res) => {
  try {
    const stats = await getStats();
    res.json(stats);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices
 * Lista de dispositivos Xinje conectados con últimos datos
 */
app.get('/api/devices', async (req, res) => {
  try {
    const HEALTH_WINDOW_MS = 60 * 1000;
    const now = Date.now();

    const mqttDevicesRaw = mqttHandler.getConnectedDevices();
    const dbDevicesRaw = await getDevices();
    const mqttDevices = Array.isArray(mqttDevicesRaw) ? mqttDevicesRaw : [];
    const dbDevices = Array.isArray(dbDevicesRaw) ? dbDevicesRaw : [];

    // Combinar dispositivos MQTT y de BD para que los creados sin mensajes aún aparezcan
    const allIds = new Set([...mqttDevices.map(d => d.id), ...dbDevices.map(d => String(d.device_name))]);
    const unifiedDevices = Array.from(allIds).map(id => {
      const mqttDev = mqttDevices.find(d => d.id === id) || { id, topics: {} };
      return mqttDev;
    });

    const devices = await Promise.all(unifiedDevices.map(async (mqttDev) => {
      const dbDev = dbDevices.find((d) => (
        String(d.device_name) === String(mqttDev.id)
        || String(d.device_id) === String(mqttDev.id)
      ));
      const latestByKey = await getLatestByKey(mqttDev.id);

      const getTopicAge = (topicName) => {
        const rawTs = mqttDev?.topics?.[topicName]?.lastUpdate;
        if (!rawTs) return Number.POSITIVE_INFINITY;
        const ts = rawTs instanceof Date ? rawTs.getTime() : new Date(rawTs).getTime();
        if (Number.isNaN(ts)) return Number.POSITIVE_INFINITY;
        return now - ts;
      };

      const runningTopics = ['events', 'pub_data', 'pub_configlist', 'write_reply'];
      const hasRunningActivity = runningTopics.some((topicName) => getTopicAge(topicName) <= HEALTH_WINDOW_MS);
      const hasCommandActivity = (
        getTopicAge('write_data') <= HEALTH_WINDOW_MS
        || getTopicAge('access_data') <= HEALTH_WINDOW_MS
      );
      const dbLastSeenTs = dbDev?.last_seen ? new Date(dbDev.last_seen).getTime() : Number.NaN;
      const hasRecentDbSeen = !Number.isNaN(dbLastSeenTs) && (now - dbLastSeenTs) <= HEALTH_WINDOW_MS;
      const shouldExposeHealth = hasRunningActivity || hasCommandActivity || hasRecentDbSeen;

      let lastData = {};

      if (latestByKey && Array.isArray(latestByKey) && latestByKey.length > 0) {
        lastData = latestByKey.reduce((acc, item) => {
          const deviceName = item.device_name || 'Dispositivo';
          if (!acc[deviceName]) {
            acc[deviceName] = {};
          }
          acc[deviceName][item.tag_name] = item.value;
          return acc;
        }, {});
      }

      return {
        ...mqttDev,
        ...dbDev,
        ...(shouldExposeHealth ? {
          health: {
            status: 'healthy',
            source: hasRunningActivity && hasCommandActivity
              ? 'runtime_and_commands'
              : hasRunningActivity
                ? 'runtime'
                : hasCommandActivity
                  ? 'commands'
                  : 'db_last_seen',
            windowMs: HEALTH_WINDOW_MS
          }
        } : {}),
        lastData: lastData,
        topics: {
          ...mqttDev.topics,
          pub_data: {
            ...(mqttDev.topics?.pub_data || {}),
            lastPayload: {
              ...(mqttDev.topics?.pub_data?.lastPayload || {}),
              Pub_Data: lastData
            }
          }
        }
      };
    }));

    res.json({
      total: devices.length,
      devices: devices
    });
  } catch (error) {
    console.error('Error en /api/devices:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/devices
 * Agrega un nuevo dispositivo al que el backend se suscribirá dinámicamente
 */
app.post('/api/devices', async (req, res) => {
  try {
    const { deviceId } = req.body;
    if (!deviceId) return res.status(400).json({ error: 'deviceId requerido' });
    
    if (mqttHandler) {
      mqttHandler.addDeviceSubscription(deviceId);
    }
    
    // Crear el dispositivo en la BD para que ya aparezca en la lista
    await saveOrUpdateDevice(deviceId, String(deviceId), 'HMI');
    
    res.json({ success: true, deviceId });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/latest
 * Últimos valores de cada variable de un dispositivo
 */
app.get('/api/devices/:deviceId/latest', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const data = await getLatestByKey(deviceId);
    
    // Convertir a formato key-value más legible
    const formatted = {};
    data.forEach(row => {
      formatted[row.tag_name] = {
        value: row.value,
        timestamp: row.timestamp
      };
    });
    
    res.json({
      deviceId,
      data: formatted,
      count: Object.keys(formatted).length
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/sensors
 * Lista de variables (sensores) por dispositivo
 */
app.get('/api/devices/:deviceId/sensors', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const sensors = await getSensors(deviceId);
    const tags = sensors.map(sensor => sensor.tag_name);

    res.json({
      deviceId,
      count: tags.length,
      tags: tags,
      sensors: sensors
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/sensors:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * PUT /api/devices/:deviceId/sensors/active
 * Define explícitamente qué tags quedan activos para el dispositivo
 * body: { tags: string[] }
 */
app.put('/api/devices/:deviceId/sensors/active', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { tags } = req.body || {};

    if (!Array.isArray(tags)) {
      return res.status(400).json({ error: 'El campo tags debe ser un arreglo de strings' });
    }

    const result = await setActiveSensors(deviceId, tags);
    res.json({
      deviceId,
      activeTags: tags,
      updated: result.updated,
      totalActive: result.totalActive
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/sensors/active:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/devices/:deviceId/sensors/active-profile/ppm
 * Activa sólo telemetry_pv_ppm y telemetry_sp_velocidad_ppm
 */
app.post('/api/devices/:deviceId/sensors/active-profile/ppm', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const result = await activatePpmProfile(deviceId);

    res.json({
      deviceId,
      activeTags: ['telemetry_pv_ppm', 'telemetry_sp_velocidad_ppm'],
      updated: result.updated,
      totalActive: result.totalActive
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/sensors/active-profile/ppm:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/history
 * Histórico de datos de un dispositivo
 */
app.get('/api/devices/:deviceId/history', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { limit = 100, key, startTime, endTime } = req.query;

    const data = await getHistoricalData({
      deviceId,
      key,
      startTime: startTime ? parseInt(startTime) : undefined,
      endTime: endTime ? parseInt(endTime) : undefined,
      limit: parseInt(limit)
    });

    res.json({
      deviceId,
      count: data.length,
      data: data
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/debug/data-sample
 * Debug: Ver cómo están guardados los datos en BD
 */
app.get('/api/debug/data-sample', (req, res) => {
  try {
    const db = require('./database').db;
    
    // Obtener últimos 5 registros
    const data = db.prepare(`
      SELECT * FROM sensor_data 
      ORDER BY id DESC 
      LIMIT 5
    `).all();
    
    // Obtener únicas claves
    const keys = db.prepare(`
      SELECT DISTINCT key FROM sensor_data 
      ORDER BY key
    `).all();
    
    // Obtener dispositivos
    const devices = db.prepare(`
      SELECT DISTINCT device_id FROM sensor_data 
      ORDER BY device_id
    `).all();
    
    res.json({
      lastRecords: data,
      uniqueKeys: keys,
      devices: devices,
      totalRecords: db.prepare('SELECT COUNT(*) as count FROM sensor_data').get().count
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/history/paginated
 * Histórico con paginación (OPTIMIZADO)
 * Query params:
 *   - page: número de página (default 0)
 *   - pageSize: registros por página (default 50, max 500)
 *   - key: (opcional) filtrar por variable específica
 *   - startTime: (opcional) timestamp inicial
 *   - endTime: (opcional) timestamp final
 */
app.get('/api/devices/:deviceId/history/paginated', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { page, pageSize, key, startTime, endTime } = req.query;

    const result = await getHistoricalDataPaginated({
      deviceId,
      page,
      pageSize,
      key,
      startTime: startTime ? parseInt(startTime) : undefined,
      endTime: endTime ? parseInt(endTime) : undefined
    });

    res.json({
      deviceId,
      ...result
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/data/latest
 * Últimos datos de todos los dispositivos
 */
app.get('/api/data/latest', async (req, res) => {
  try {
    const devices = await getDevices();
    const result = {};

    await Promise.all(devices.map(async (device) => {
      const data = await getLatestByKey(device.device_id);
      const formatted = {};

      data.forEach(row => {
        formatted[row.tag_name] = {
          value: row.value,
          timestamp: row.timestamp
        };
      });

      result[device.device_id] = formatted;
    }));

    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/devices/:deviceId/command
 * Enviar comando a un dispositivo HMI Xinje
 * 
 * Body ejemplo:
 * {
 *   "Device1": {
 *     "variable1": 100,
 *     "variable2": "ON"
 *   }
 * }
 */
app.post('/api/devices/:deviceId/command', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const commandData = req.body;
    
    if (!commandData || Object.keys(commandData).length === 0) {
      return res.status(400).json({ error: 'Comando vacío' });
    }
    
    // Enviar comando vía MQTT
    await mqttHandler.sendCommand(deviceId, commandData);
    
    // Guardar en historial
    saveCommand(deviceId, commandData);
    
    res.json({
      success: true,
      deviceId,
      command: commandData,
      timestamp: Date.now()
    });
  } catch (error) {
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

/**
 * POST /api/test/publish
 * Endpoint de prueba para simular datos del HMI
 */
app.post('/api/test/publish', (req, res) => {
  const testData = {
    Unix: Date.now(),
    Version: 'V1.0',
    Pub_Data: {
      Device1: {
        temperature: Math.random() * 100,
        pressure: Math.random() * 200,
        status: 'OK'
      }
    }
  };
  
  mqttHandler.handleDataPublish('TEST001+1234', testData);
  
  res.json({
    success: true,
    message: 'Datos de prueba publicados',
    data: testData
  });
});

/**
 * POST /api/devices/:deviceId/access_data
 * Solicitar datos históricos al HMI (access_data)
 * Body ejemplo:
 * {
 *   "Access_Data": {
 *     "Device1": {
 *       "variable": "nombre_variable",
 *       "start": 1614576888000,
 *       "end": 1614577899000
 *     }
 *   }
 * }
 */
app.post('/api/devices/:deviceId/access_data', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const queryData = req.body;
    if (!queryData || Object.keys(queryData).length === 0) {
      return res.status(400).json({ error: 'Consulta vacía' });
    }
    await mqttHandler.requestAccessData(deviceId, queryData);
    res.json({
      success: true,
      deviceId,
      query: queryData,
      timestamp: Date.now()
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/devices/:deviceId/history/1min
 * Datos agregados cada 1 minuto
 */
app.get('/api/devices/:deviceId/history/1min', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { limit = 100 } = req.query;
    
    const data = await getAggregated1min(deviceId, parseInt(limit));
    
    res.json({
      deviceId,
      interval: '1 minute',
      count: data.length,
      data: data
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/history/1min:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/history/5min
 * Datos agregados cada 5 minutos
 */
app.get('/api/devices/:deviceId/history/5min', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { limit = 100 } = req.query;
    
    const data = await getAggregated5min(deviceId, parseInt(limit));
    
    res.json({
      deviceId,
      interval: '5 minutes',
      count: data.length,
      data: data
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/history/5min:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/history/10min
 * Datos agregados cada 10 minutos
 */
app.get('/api/devices/:deviceId/history/10min', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { limit = 100 } = req.query;
    
    const data = await getAggregated10min(deviceId, parseInt(limit));
    
    res.json({
      deviceId,
      interval: '10 minutes',
      count: data.length,
      data: data
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/history/10min:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/history/1hour
 * Datos agregados cada 1 hora
 */
app.get('/api/devices/:deviceId/history/1hour', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { limit = 24 } = req.query;
    
    const data = await getAggregated1hour(deviceId, parseInt(limit));
    
    res.json({
      deviceId,
      interval: '1 hour',
      count: data.length,
      data: data
    });
  } catch (error) {
    console.error('[ERROR] Error en /api/devices/:deviceId/history/1hour:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/events
 * Historial de eventos de configuración y auditoría de una máquina
 * Query params:
 *   - event_type : filtrar por tipo (STATE_CHANGE, SETPOINT_CHANGE, ALARM_TRIGGER, ...)
 *   - parameter  : filtrar por variable (event_sp_temp_mordaza_H, ...)
 *   - startTime  : timestamp epoch ms inicial
 *   - endTime    : timestamp epoch ms final
 *   - limit      : máx registros (default 200, máx 1000)
 */
app.get('/api/devices/:deviceId/events', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { event_type, parameter, startTime, endTime, limit } = req.query;

    const data = await getMachineEvents(deviceId, {
      event_type,
      parameter,
      startTime: startTime ? parseInt(startTime) : undefined,
      endTime:   endTime   ? parseInt(endTime)   : undefined,
      limit:     limit     ? parseInt(limit)      : 200
    });

    res.json({
      deviceId,
      count: data.length,
      events: data
    });
  } catch (error) {
    console.error('[ERROR] /api/devices/:deviceId/events:', error.message);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/devices/:deviceId/events/summary
 * Último valor conocido de cada parámetro de configuración
 */
app.get('/api/devices/:deviceId/events/summary', async (req, res) => {
  try {
    const { deviceId } = req.params;

    // Obtener el evento más reciente de cada parámetro
    const all = await getMachineEvents(deviceId, { limit: 1000 });
    const latest = {};
    for (const ev of all) {
      if (!latest[ev.parameter]) {
        latest[ev.parameter] = ev; // ya viene ordenado DESC
      }
    }

    res.json({
      deviceId,
      count: Object.keys(latest).length,
      summary: latest
    });
  } catch (error) {
    console.error('[ERROR] /api/devices/:deviceId/events/summary:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// ========================================
// WEBSOCKET
// ========================================

io.on('connection', (socket) => {
  console.log(`📱 Cliente WebSocket conectado: ${socket.id}`);
  
  // Enviar dispositivos conectados al cliente nuevo
  socket.emit('devices-list', mqttHandler.getConnectedDevices());
  
  socket.on('disconnect', () => {
    console.log(`📱 Cliente WebSocket desconectado: ${socket.id}`);
  });
  
  // Permitir al cliente solicitar datos específicos
  socket.on('request-device-data', (deviceId) => {
    const data = getLatestByKey(deviceId);
    socket.emit('device-data-response', { deviceId, data });
  });
});

// Emitir lista de dispositivos cada 30 segundos
setInterval(() => {
  io.emit('devices-list', mqttHandler.getConnectedDevices());
}, 30000);

// ========================================
// ERROR HANDLERS
// ========================================

app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint no encontrado' });
});

app.use((error, req, res, next) => {
  console.error('[ERROR] Error del servidor:', error);
  res.status(500).json({ error: 'Error interno del servidor' });
});

// ========================================
// INICIO DEL SERVIDOR
// ========================================

async function startServer() {
  try {
    // Inicializar PostgreSQL
    console.log('[DB] Inicializando conexión a PostgreSQL...');
    await initConnectionPool();
    await initDatabase();
    await refreshDevicesOnlineStatus(DEVICE_STATUS_WINDOW_MINUTES);
    console.log('[DB] PostgreSQL inicializado correctamente');

    setInterval(async () => {
      try {
        const summary = await refreshDevicesOnlineStatus(DEVICE_STATUS_WINDOW_MINUTES);
        console.log(`[DB] Sync status dispositivos -> online: ${summary.online}, offline: ${summary.offline}`);
      } catch (error) {
        console.error('[ERROR] Fallo en sync de estado de dispositivos:', error.message);
      }
    }, DEVICE_STATUS_SYNC_INTERVAL_MS);

    if (ENABLE_APP_AGGREGATION_JOBS) {
      startAggregationJobs();
      console.log('[DB] Jobs de agregación internos habilitados');
    } else {
      console.log('[DB] Jobs de agregación internos deshabilitados por configuración');
    }
    
    // Intentar conectar a MQTT (no crítico si falla)
    try {
      await mqttHandler.connect();

      // Suscribir automaticamente todos los dispositivos existentes en BD
      // para evitar que queden "inactivos" por no estar en MQTT_DEVICE_IDS.
      const knownDevices = await getDevices();
      knownDevices.forEach((device) => {
        const id = String(device.device_name || '').trim();
        if (id) {
          mqttHandler.addDeviceSubscription(id);
        }
      });

      console.log('[MQTT] Conectado correctamente');
    } catch (mqttError) {
      console.warn('[WARN] MQTT no disponible - El servidor funcionara sin comunicacion MQTT');
      console.warn('[WARN] Para usar MQTT, instala y ejecuta Mosquitto broker');
    }
    
    // Iniciar servidor HTTP
    server.listen(PORT, () => {
      console.log('');
      console.log('='.repeat(60));
      console.log('[SERVIDOR] Servidor Xinje IoT Iniciado');
      console.log('='.repeat(60));
      console.log(`[HTTP] http://localhost:${PORT}`);
      console.log(`[MQTT] MQTT Broker: ${MQTT_BROKER}`);
      console.log(`[WS] WebSocket: ws://localhost:${PORT}`);
      console.log('');
      console.log('[ENDPOINTS] Disponibles:');
      console.log('   GET  /api/health');
      console.log('   GET  /api/stats');
      console.log('   GET  /api/devices');
      console.log('   GET  /api/devices/:id/latest');
      console.log('   GET  /api/devices/:id/history');
      console.log('   GET  /api/devices/:id/history/1min');
      console.log('   GET  /api/devices/:id/history/5min');
      console.log('   GET  /api/devices/:id/history/10min');
      console.log('   GET  /api/devices/:id/history/1hour');
      console.log('   GET  /api/devices/:id/events               <- NUEVO');
      console.log('   GET  /api/devices/:id/events/summary       <- NUEVO');
      console.log('   POST /api/devices/:id/command');
      console.log('='.repeat(60));
      console.log('');
    });
    
  } catch (error) {
    console.error('[ERROR] Error al iniciar el servidor:', error);
    process.exit(1);
  }
}

// Manejo de cierre graceful
process.on('SIGINT', () => {
  console.log('\n[SERVIDOR] Cerrando servidor...');
  stopAggregationJobs();
  mqttHandler.disconnect();
  server.close(() => {
    console.log('[SERVIDOR] Servidor cerrado correctamente');
    process.exit(0);
  });
});

// Iniciar
startServer();
