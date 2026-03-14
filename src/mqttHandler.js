/**
 * Manejador MQTT para Xinje TouchWin Pro HMI
 * 
 * Este módulo maneja la comunicación MQTT con los dispositivos Xinje HMI
 * siguiendo el formato de topics y payloads especificado en la documentación.
 * 
 * Formato de Topics: ID+PWD/nombre_del_topico
 * - ID+PWD/pub_data: Datos en tiempo real del HMI (suscripción)
 * - ID+PWD/write_data: Comandos hacia el HMI (publicación)
 * - ID+PWD/access_data: Consulta de datos históricos (publicación)
 * - ID+PWD/write_reply: Respuestas del HMI a comandos
 */

const mqtt = require('mqtt');
const { saveData, saveConfig, saveMachineEvent } = require('./database');

class XinjeMQTTHandler {
  constructor(brokerUrl, options = {}) {
    const configuredDeviceIds = options.deviceIds
      || process.env.MQTT_DEVICE_IDS
      || '441095104B78F267112345678,441095104B78F267112343457';

    const parsedDeviceIds = String(configuredDeviceIds)
      .split(',')
      .map((id) => id.trim())
      .filter(Boolean);

    this.brokerUrl = brokerUrl;
    this.options = {
      clientId: options.clientId || 'xinje_backend_' + Math.random().toString(16).substr(2, 8),
      username: options.username,
      password: options.password,
      clean: true,
      reconnectPeriod: 0,  // Deshabilitar reconexión automática
      connectTimeout: 5 * 1000  // Reducir timeout a 5 segundos
    };
    
    this.client = null;
    this.subscribers = [];
    this.deviceCache = new Map(); // Cache de dispositivos conectados
    this.valueCache = new Map(); // Cache para detectar cambios en valores
    this.targetDeviceIds = Array.from(new Set(parsedDeviceIds));
    this.healthWindowMs = parseInt(process.env.DEVICE_HEALTH_WINDOW_MS || '60000', 10);
    this.isConnected = false;
  }

  /**
   * Conectar al broker MQTT
   */
  connect() {
    return new Promise((resolve, reject) => {
      console.log(`[MQTT] Conectando a MQTT Broker: ${this.brokerUrl}`);
      
      this.client = mqtt.connect(this.brokerUrl, this.options);

      this.client.on('connect', () => {
        console.log('[MQTT] Conectado exitosamente al MQTT Broker');
        this.isConnected = true;
        
        // Suscribirse a todos los topics relevantes de Xinje
        this.subscribeToXinjeTopics();
        
        resolve();
      });

      this.client.on('error', (error) => {
        console.error('[ERROR] Error MQTT:', error.message);
        this.isConnected = false;
        reject(error);
      });

      this.client.on('close', () => {
        if (this.isConnected) {
          console.log('[MQTT] Conexion MQTT cerrada');
          this.isConnected = false;
        }
      });

      this.client.on('message', (topic, message) => {
        this.handleMessage(topic, message);
      });
    });
  }

  /**
   * Suscribirse a todos los topics de Xinje usando wildcards
   */

  subscribeToXinjeTopics() {
    const topics = [];

    if (this.targetDeviceIds.length > 0) {
      this.targetDeviceIds.forEach((deviceId) => {
        topics.push(`${deviceId}/events`);       // único tópico: telemetría + eventos
        topics.push(`${deviceId}/write_reply`);  // respuestas a comandos
      });
    } else {
      // Fallback: escuchar cualquier ID si no se configuró lista.
      topics.push('+/events');
      topics.push('+/write_reply');
    }

    console.log(`[MQTT] IDs objetivo: ${this.targetDeviceIds.length > 0 ? this.targetDeviceIds.join(', ') : 'ALL (+/...)'}`);

    topics.forEach(topic => {
      this.client.subscribe(topic, { qos: 2 }, (err) => {
        if (err) {
          console.error(`[ERROR] Error al suscribirse a ${topic}:`, err);
        } else {
          console.log(`[MQTT] Suscrito a: ${topic} (QoS 2)`);
        }
      });
    });
  }

  /**
   * Consultar datos históricos o guardados (access_data)
   * Topic: ID+PWD/access_data
   * @param {string} deviceId
   * @param {object} queryData - Estructura según HMI
   */
  requestAccessData(deviceId, queryData) {
    return new Promise((resolve, reject) => {
      const topic = `${deviceId}/access_data`;
      const payload = {
        Unix: Date.now(),
        Version: 'V1.0',
        ...queryData
      };
      const message = JSON.stringify(payload);
      this.client.publish(topic, message, { qos: 1 }, (err) => {
        if (err) {
          console.error(`[ERROR] Error solicitando access_data a ${deviceId}:`, err);
          reject(err);
        } else {
          this.updateDeviceCache(deviceId, 'access_data', payload);
          console.log(`[MQTT] Solicitud access_data enviada a ${deviceId}`);
          resolve();
        }
      });
    });
  }

  /**
   * Manejar mensajes entrantes del HMI Xinje
   */
  handleMessage(topic, message) {
    console.log(`[RAW MQTT] Topic: ${topic} | Longitud: ${message.length} bytes`);
    
    try {
      const messageStr = message.toString('utf8');
      let payload;
      
      try {
        payload = JSON.parse(messageStr);
      } catch (e) {
        console.warn(`[WARN] Mensaje no es JSON valido en ${topic}. Contenido: ${messageStr.substring(0, 100)}...`);
        return;
      }
      const [deviceId, topicType] = topic.split('/');
      
      console.log(`[MQTT] Mensaje recibido de ${deviceId} (${topicType})`);
      
      // El HMI Xinje envuelve los datos en un array Variant
      // Extraer el contenido del array Variant
      if (payload.Variant && Array.isArray(payload.Variant) && payload.Variant.length > 0) {
        payload = payload.Variant[0];
      }
      
      // Validar estructura del mensaje según documentación Xinje
      if (!this.validateXinjePayload(payload)) {
        console.warn('[WARN] Payload no valido:', payload);
        return;
      }

      // Actualizar cache de dispositivos
      this.updateDeviceCache(deviceId, topicType, payload);

      // Procesar según el tipo de topic
      switch (topicType) {
        case 'pub_configlist':
          this.handleConfigList(deviceId, payload);
          break;

        case 'events': {
          // Diferenciar por message_type dentro del payload
          const msgType = payload.message_type;
          if (msgType === 'telemetry') {
            this.handleTelemetry(deviceId, payload);
          } else if (msgType === 'event') {
            this.handleEvent(deviceId, payload);
          } else {
            // Compatibilidad con simulador antiguo (pub_data)
            this.handleDataPublish(deviceId, payload);
          }
          break;
        }

        case 'pub_data':
          this.handleDataPublish(deviceId, payload);
          break;

        case 'write_reply':
          this.handleWriteReply(deviceId, payload);
          break;

        default:
          console.log(`[INFO] Topic desconocido: ${topicType}`);
      }

      // Notificar a suscriptores (WebSocket)
      this.notifySubscribers({ topic, deviceId, topicType, payload });

    } catch (error) {
      console.error('[ERROR] Error procesando mensaje MQTT:', error);
      console.error('Topic:', topic);
      console.error('Message:', message.toString());
    }
  }

  /**
   * Validar payload según especificación Xinje
   * Estructura esperada: { Unix: timestamp, Version: "V1.0", [Configlist|Pub_Data]: {...} }
   */
  validateXinjePayload(payload) {
    if (!payload || typeof payload !== 'object') return false;
    if (!payload.Unix || !payload.Version) return false;
    if (payload.Version !== 'V1.0' && payload.Version !== 'V2.0') {
      console.warn(`[WARN] Version de protocolo no reconocida: ${payload.Version}`);
    }
    return true;
  }

  /**
   * Actualizar cache de dispositivos conectados
   */
  updateDeviceCache(deviceId, topicType, payload) {
    if (!this.deviceCache.has(deviceId)) {
      this.deviceCache.set(deviceId, {
        id: deviceId,
        firstSeen: new Date(),
        lastSeen: new Date(),
        topics: {},
        status: 'online'
      });
      console.log(`[MQTT] Nuevo dispositivo detectado: ${deviceId}`);
    }

    const device = this.deviceCache.get(deviceId);
    device.lastSeen = new Date();
    device.topics[topicType] = {
      lastUpdate: new Date(),
      lastPayload: payload
    };
  }

  /**
   * Manejar lista de configuración del dispositivo
   */
  handleConfigList(deviceId, payload) {
    console.log(`[MQTT] Configuracion recibida de ${deviceId}`);
    
    if (payload.Configlist) {
      saveConfig(deviceId, payload);
      console.log(`[DB] Configuracion guardada para ${deviceId}`);
    }
  }

  /**
   * Manejar datos publicados por el HMI (formato antiguo V1.0)
   */
  handleDataPublish(deviceId, payload) {
    console.log(`[MQTT] Datos recibidos de ${deviceId} a las ${new Date().toISOString()}`);
    
    if (payload.Pub_Data) {
      const pubData = payload.Pub_Data;
      const deviceName = Object.keys(pubData)[0];
      
      if (!deviceName) return;
      
      const deviceData = pubData[deviceName];
      const timestamp = parseInt(payload.Unix) || Date.now();
      
      const varsToProcess = ['cantidad_productos', 'temperatura'];
      
      varsToProcess.forEach(key => {
        if (key in deviceData) {
          const value = deviceData[key];
          const cacheKey = `${deviceId}:${key}`;
          const previousValue = this.valueCache.get(cacheKey);
          
          if (previousValue !== value) {
            saveData({
              deviceId: deviceId,
              deviceName: String(deviceId),
              key: key,
              value: value,
              timestamp: timestamp,
              version: payload.Version
            });
            
            this.valueCache.set(cacheKey, value);
            console.log(`[DB] Guardado: ${key} = ${value} (anterior: ${previousValue})`);
          }
        }
      });
    }
  }

  /**
   * Manejar telemetría continua (V2.0 message_type: telemetry)
   * Procesa todas las claves con prefijo telemetry_* del Pub_Data
   */
  handleTelemetry(deviceId, payload) {
    if (!payload.Pub_Data) return;

    const deviceName = Object.keys(payload.Pub_Data)[0];
    if (!deviceName) return;

    const deviceData  = payload.Pub_Data[deviceName];
    const timestamp   = parseInt(payload.Unix) || Date.now();

    const telemetryKeys = Object.keys(deviceData).filter(k => k.startsWith('telemetry_'));

    telemetryKeys.forEach(key => {
      const value     = deviceData[key];
      const cacheKey  = `${deviceId}:${key}`;
      const prevValue = this.valueCache.get(cacheKey);

      if (prevValue !== value) {
        // El tag_name guardado en BD es la clave tal como viene (telemetry_*)
        saveData({
          deviceId:   deviceId,
          deviceName: String(deviceId),
          key:        key,
          value:      typeof value === 'boolean' ? (value ? 1 : 0) : value,
          timestamp:  timestamp,
          version:    payload.Version
        });
        this.valueCache.set(cacheKey, value);
      }
    });

    console.log(`[TELEMETRY] ${deviceId} | ${telemetryKeys.length} vars | t=${new Date(timestamp).toISOString()}`);
  }

  /**
   * Manejar eventos de configuración / auditoría (V2.0 message_type: event)
   * Persistir en machine_events con los campos event_* del payload
   */
  handleEvent(deviceId, payload) {
    saveMachineEvent(deviceId, {
      unix_ts:    parseInt(payload.Unix) || Date.now(),
      event_type: payload.event_type,
      parameter:  payload.parameter,   // ya viene con prefijo event_
      old_value:  payload.old_value,
      new_value:  payload.new_value,
      details:    payload.details
    });
  }

  /**
   * Manejar respuestas a comandos de escritura
   */
  handleWriteReply(deviceId, payload) {
    console.log(`[MQTT] Respuesta de escritura recibida de ${deviceId}:`, payload);
    // Aquí puedes implementar lógica para manejar confirmaciones de comandos
  }

  /**
   * Enviar comando al HMI (escribir datos)
   * Topic: ID+PWD/write_data
   */
  sendCommand(deviceId, commandData) {
    return new Promise((resolve, reject) => {
      if (!this.client) {
        reject(new Error('Cliente MQTT no inicializado'));
        return;
      }

      const topic = `${deviceId}/write_data`;
      
      const payload = {
        Unix: Date.now(),
        Version: 'V1.0',
        Write_Data: commandData
      };

      const message = JSON.stringify(payload);

      this.client.publish(topic, message, { qos: 1 }, (err) => {
        if (err) {
          console.error(`[ERROR] Error enviando comando a ${deviceId}:`, err);
          reject(err);
        } else {
          this.updateDeviceCache(deviceId, 'write_data', payload);
          console.log(`[MQTT] Comando enviado a ${deviceId}`);
          resolve();
        }
      });
    });
  }

  /**
   * Suscribir callback para recibir notificaciones en tiempo real
   */
  subscribe(callback) {
    this.subscribers.push(callback);
    return () => {
      this.subscribers = this.subscribers.filter(cb => cb !== callback);
    };
  }

  /**
   * Notificar a todos los suscriptores
   */
  notifySubscribers(data) {
    this.subscribers.forEach(callback => {
      try {
        callback(data);
      } catch (error) {
        console.error('[ERROR] Error en callback de suscriptor:', error);
      }
    });
  }

  buildDeviceHealth(topics, nowTs) {
    const getTopicAge = (topicName) => {
      const rawTs = topics?.[topicName]?.lastUpdate;
      if (!rawTs) return Number.POSITIVE_INFINITY;
      const ts = rawTs instanceof Date ? rawTs.getTime() : new Date(rawTs).getTime();
      if (Number.isNaN(ts)) return Number.POSITIVE_INFINITY;
      return nowTs - ts;
    };

    const runningTopics = ['events', 'pub_data', 'pub_configlist', 'write_reply'];
    const hasRunningActivity = runningTopics.some((topicName) => getTopicAge(topicName) <= this.healthWindowMs);
    const hasCommandActivity = getTopicAge('write_data') <= this.healthWindowMs;
    const hasAccessActivity = getTopicAge('access_data') <= this.healthWindowMs;

    if (!hasRunningActivity && !hasCommandActivity && !hasAccessActivity) {
      return null;
    }

    let source = 'runtime';
    if (hasRunningActivity && (hasCommandActivity || hasAccessActivity)) {
      source = 'runtime_and_commands';
    } else if (hasCommandActivity || hasAccessActivity) {
      source = 'commands';
    }

    return {
      status: 'healthy',
      source,
      windowMs: this.healthWindowMs
    };
  }

  /**
   * Obtener lista de dispositivos conectados
   */
  getConnectedDevices() {
    const devices = [];
    const now = Date.now();
    const TIMEOUT = 60000; // 1 minuto sin actividad = offline

    this.deviceCache.forEach((device, deviceId) => {
      const timeSinceLastSeen = now - device.lastSeen.getTime();
      const health = this.buildDeviceHealth(device.topics, now);

      devices.push({
        ...device,
        status: timeSinceLastSeen < TIMEOUT ? 'online' : 'offline',
        lastSeenAgo: timeSinceLastSeen,
        ...(health ? { health } : {})
      });
    });

    return devices;
  }

  /**
   * Desconectar del broker
   */
  disconnect() {
    if (this.client) {
      console.log('[MQTT] Desconectando MQTT...');
      this.client.end();
    }
  }
}

module.exports = XinjeMQTTHandler;
