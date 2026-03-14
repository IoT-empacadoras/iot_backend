/**
 * Simulador VFFS - Empacadora Automática ReadyPackers
 *
 * Publica en un único tópico (limitación del HMI):
 *   {DEVICE_ID}/events  → todo el tráfico
 *
 * Se diferencia por el campo `message_type`:
 *   "telemetry" → datos continuos cada 10 s
 *   "event"     → cambio de configuración / alarma (por evento)
 *
 * Esquema de eventos:
 *   event_type     | parameter                       | old_value | new_value
 *   STATE_CHANGE   | event_is_running                | bool      | bool
 *   RECIPE_LOAD    | event_recipe_id                 | number    | number
 *   SETPOINT_CHANGE| event_sp_temp_mordaza_H/V       | number    | number
 *   SETPOINT_CHANGE| event_sp_peso_objetivo          | number    | number
 *   SETPOINT_CHANGE| event_sp_dwell_time_ms          | number    | number
 *   SETPOINT_CHANGE| event_sp_velocidad_ppm          | number    | number
 *   TARE_ACTION    | event_tare_offset_kg            | number    | number
 *   ALARM_TRIGGER  | event_pressure_ok               | bool      | bool
 *   ALARM_CLEAR    | event_pressure_ok               | bool      | bool
 *   CALIBRATION    | event_calibrated_ok             | bool      | bool
 */

const mqtt = require('mqtt');

// ============================================================
// CONFIGURACIÓN
// ============================================================

const BROKER_URL = process.env.MQTT_BROKER || 'mqtt://broker.hivemq.com:1883';
//const DEVICE_ID  = process.env.DEVICE_ID   || '441095104B78F267112345678';

const DEVICE_ID  = process.env.DEVICE_ID   || '441095104B78F267112343457'; // ID de dispositivo simulado (debe coincidir con el configurado en HMI)

const TOPIC = `${DEVICE_ID}/events`; // único tópico permitido por el HMI

const TELEMETRY_INTERVAL_MS = 10_000;  // cada 10 s
const EVENT_CHECK_INTERVAL_MS = 15_000; // revisa si hay evento cada 15 s

// ============================================================
// ESTADO DE LA MÁQUINA (se va mutando durante la simulación)
// ============================================================

const machineState = {
  is_running: false,
  recipe_id: 1,

  // SetPoints configurados en HMI
  sp_temp_mordaza_H:  150.0,   // °C
  sp_temp_mordaza_V:  145.0,   // °C
  sp_peso_objetivo:   500.0,   // g
  sp_dwell_time_ms:   350.0,   // ms
  sp_velocidad_ppm:    45.0,   // paquetes/min

  // Estado interno de alarmas
  pressure_ok:      true,
  calibrated_ok:    true,
  tare_offset_kg:   0.00,

  // Valores de proceso actuales (PV)
  pv_temp_mordaza_H: 148.5,
  pv_temp_mordaza_V: 143.2,
  pv_peso_promedio:  499.0,
  pv_presion_aire:     6.2,
  pv_ppm:             43.0,
  pv_tiempo_ciclo_ms: 1320,
};

// ============================================================
// CONEXIÓN MQTT
// ============================================================

console.log(`[VFFS-SIM] Conectando al broker: ${BROKER_URL}`);
const client = mqtt.connect(BROKER_URL, {
  clientId: `vffs_sim_${Math.random().toString(16).slice(2, 10)}`,
  clean: true,
});

client.on('connect', () => {
  console.log('[VFFS-SIM] Conectado al broker MQTT');
  console.log(`[VFFS-SIM] Tópico único → ${TOPIC}\n`);

  // Arrancar máquina simulada
  setTimeout(() => triggerEvent('STATE_CHANGE', 'is_running', false, true, 'Inicio de produccion'), 2000);

  setInterval(publishTelemetry, TELEMETRY_INTERVAL_MS);
  setInterval(maybePublishEvent, EVENT_CHECK_INTERVAL_MS);

  publishTelemetry(); // primer dato inmediato
});

client.on('error', (err) => {
  console.error('[VFFS-SIM][ERROR]', err.message);
  process.exit(1);
});

// ============================================================
// TELEMETRÍA — datos continuos
// ============================================================

function publishTelemetry() {
  if (!machineState.is_running) return; // no publica si la máquina está parada

  updateProcessValues();

  const payload = {
    Unix: Date.now().toString(),
    Version: 'V2.0',
    message_type: 'telemetry',          // ← diferenciador
    Pub_Data: {
      Empacadora_VFFS: {
        // Variables de proceso continuas
        telemetry_pv_temp_mordaza_H:  machineState.pv_temp_mordaza_H,
        telemetry_pv_temp_mordaza_V:  machineState.pv_temp_mordaza_V,
        telemetry_pv_peso_promedio:   machineState.pv_peso_promedio,
        telemetry_pv_presion_aire:    machineState.pv_presion_aire,
        telemetry_pv_ppm:             machineState.pv_ppm,
        telemetry_pv_tiempo_ciclo_ms: machineState.pv_tiempo_ciclo_ms,

        // SetPoints activos en este momento
        telemetry_sp_temp_mordaza_H:  machineState.sp_temp_mordaza_H,
        telemetry_sp_temp_mordaza_V:  machineState.sp_temp_mordaza_V,
        telemetry_sp_peso_objetivo:   machineState.sp_peso_objetivo,
        telemetry_sp_velocidad_ppm:   machineState.sp_velocidad_ppm,

        // Estados booleanos
        telemetry_is_running:    machineState.is_running,
        telemetry_pressure_ok:   machineState.pressure_ok,
        telemetry_calibrated_ok: machineState.calibrated_ok,
      }
    }
  };

  publish(TOPIC, payload);
  console.log(
    `[TELEMETRY] telemetry_pv_ppm=${machineState.pv_ppm} | ` +
    `telemetry_pv_temp_mordaza_H=${machineState.pv_temp_mordaza_H}°C | ` +
    `telemetry_pv_temp_mordaza_V=${machineState.pv_temp_mordaza_V}°C | ` +
    `telemetry_pv_peso_promedio=${machineState.pv_peso_promedio}g | ` +
    `telemetry_pv_presion_aire=${machineState.pv_presion_aire}bar`
  );
}

function updateProcessValues() {
  const s = machineState;

  // Temperatura sigue al SetPoint con ±2°C de ruido
  s.pv_temp_mordaza_H = clamp(s.sp_temp_mordaza_H + randFloat(-2, 2), 100, 220);
  s.pv_temp_mordaza_V = clamp(s.sp_temp_mordaza_V + randFloat(-2, 2), 100, 220);

  // Peso promedio sigue al objetivo con ±3g de variación
  s.pv_peso_promedio  = clamp(s.sp_peso_objetivo + randFloat(-3, 3),     50, 2000);

  // Presión oscila alrededor de 6 bar (baja si hay alarma activa)
  const pressureBase  = s.pressure_ok ? 6.2 : 4.6;
  s.pv_presion_aire   = clamp(pressureBase + randFloat(-0.3, 0.3),      0, 10);

  // PPM oscila alrededor del SP de velocidad
  s.pv_ppm            = clamp(s.sp_velocidad_ppm + randFloat(-3, 3),    0, 120);

  // Tiempo de ciclo inversamente proporcional a PPM
  s.pv_tiempo_ciclo_ms = Math.round(60000 / Math.max(s.pv_ppm, 1));
}

// ============================================================
// EVENTOS — se disparan aleatoriamente para simular operarios
// ============================================================

// Lista de generadores de eventos posibles (se elige uno al azar si procede)
const eventGenerators = [
  // Cambio de SetPoint de temperatura (30 % de probabilidad si está corriendo)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.30) return;
    const param   = Math.random() > 0.5 ? 'sp_temp_mordaza_H' : 'sp_temp_mordaza_V';
    const oldVal  = machineState[param];
    const newVal  = clamp(+(oldVal + randFloat(-10, 10)).toFixed(1), 120, 200);
    triggerEvent('SETPOINT_CHANGE', param, oldVal, newVal, 'Ajuste desde HMI');
  },

  // Cambio de peso objetivo (15 %)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.15) return;
    const oldVal = machineState.sp_peso_objetivo;
    const newVal = clamp(+(oldVal + randFloat(-5, 5)).toFixed(1), 50, 2000);
    triggerEvent('SETPOINT_CHANGE', 'sp_peso_objetivo', oldVal, newVal, 'Ajuste por control de calidad');
  },

  // Cambio de dwell time (10 %)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.10) return;
    const oldVal = machineState.sp_dwell_time_ms;
    const newVal = clamp(+(oldVal + randFloat(-50, 50)).toFixed(0), 200, 800);
    triggerEvent('SETPOINT_CHANGE', 'sp_dwell_time_ms', oldVal, newVal, 'Ajuste tiempo de sellado');
  },

  // Cambio de velocidad ppm (10 %)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.10) return;
    const oldVal = machineState.sp_velocidad_ppm;
    const newVal = clamp(+(oldVal + randFloat(-5, 5)).toFixed(1), 10, 90);
    triggerEvent('SETPOINT_CHANGE', 'sp_velocidad_ppm', oldVal, newVal, 'Ajuste de velocidad de produccion');
  },

  // Tara de báscula (5 %)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.05) return;
    const oldOffset = +randFloat(0.01, 0.05).toFixed(3);
    triggerEvent('TARE_ACTION', 'tare_offset_kg', oldOffset, 0.00, 'Tara de bascula por operario');
    machineState.tare_offset_kg = 0.00;
  },

  // Alarma de presión (8 % si está corriendo y presión OK)
  () => {
    if (!machineState.is_running || !machineState.pressure_ok) return;
    if (Math.random() > 0.08) return;
    triggerEvent('ALARM_TRIGGER', 'pressure_ok', true, false, 'Caida de presion neumatica por debajo de 5 bar');
    machineState.pressure_ok = false;

    // Auto-recuperar en 20–40 s
    setTimeout(() => {
      if (!machineState.pressure_ok) {
        triggerEvent('ALARM_CLEAR', 'pressure_ok', false, true, 'Presion restablecida por operario');
        machineState.pressure_ok = true;
      }
    }, randInt(20_000, 40_000));
  },

  // Calibración de celda de carga (3 %)
  () => {
    if (!machineState.is_running) return;
    if (Math.random() > 0.03) return;
    triggerEvent('CALIBRATION', 'calibrated_ok', false, true, 'Recalibracion de celda de carga');
    machineState.calibrated_ok = true;
  },

  // Cambio de receta (2 % – para la máquina primero)
  () => {
    if (Math.random() > 0.02) return;
    const oldRecipe = machineState.recipe_id;
    const newRecipe = oldRecipe === 1 ? 2 : 1;
    if (machineState.is_running) {
      triggerEvent('STATE_CHANGE', 'is_running', true, false, 'Parada para cambio de receta');
      machineState.is_running = false;
    }
    setTimeout(() => {
      machineState.recipe_id = newRecipe;
      triggerEvent('RECIPE_LOAD', 'recipe_id', oldRecipe, newRecipe, `Cambio a receta ${newRecipe}`);
      // Nuevos SetPoints al cambiar receta
      machineState.sp_temp_mordaza_H = newRecipe === 2 ? 160.0 : 150.0;
      machineState.sp_temp_mordaza_V = newRecipe === 2 ? 155.0 : 145.0;
      machineState.sp_peso_objetivo  = newRecipe === 2 ? 250.0 : 500.0;
      setTimeout(() => {
        triggerEvent('STATE_CHANGE', 'is_running', false, true, 'Rearranque con nueva receta');
        machineState.is_running = true;
      }, 5000);
    }, 3000);
  },
];

function maybePublishEvent() {
  // Ejecuta todos los generadores; cada uno decide internamente si dispara
  for (const gen of eventGenerators) gen();
}

// ============================================================
// PUBLICAR UN EVENTO
// ============================================================

function triggerEvent(eventType, parameter, oldValue, newValue, details) {
  // El nombre de la variable en el JSON lleva prefijo event_
  const jsonParameter = `event_${parameter}`;

  const payload = {
    Unix: Date.now().toString(),
    Version: 'V2.0',
    message_type: 'event',              // ← diferenciador
    event_type: eventType,
    parameter:  jsonParameter,          // ← event_nombre_variable
    old_value:  oldValue,
    new_value:  newValue,
    details:    details,
  };

  // Usar nombre corto (sin prefijo) para actualizar estado interno
  if (parameter in machineState) {
    machineState[parameter] = newValue;
  }

  publish(TOPIC, payload);
  console.log(
    `[EVENT] ${eventType.padEnd(16)} | ${jsonParameter.padEnd(28)} | ` +
    `${String(oldValue).padStart(8)} → ${String(newValue).padStart(8)} | ${details}`
  );
}

// ============================================================
// HELPERS
// ============================================================

function publish(topic, payload) {
  client.publish(topic, JSON.stringify(payload), { qos: 1 }, (err) => {
    if (err) console.error(`[ERROR] Publicando en ${topic}:`, err.message);
  });
}

function clamp(val, min, max) {
  return Math.max(min, Math.min(max, val));
}

function randFloat(min, max) {
  return parseFloat((Math.random() * (max - min) + min).toFixed(2));
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// ============================================================
// CIERRE GRACIOSO
// ============================================================

process.on('SIGINT', () => {
  console.log('\n[VFFS-SIM] Deteniendo simulador...');
  client.end(() => {
    console.log('[VFFS-SIM] Conexión cerrada');
    process.exit(0);
  });
});

console.log('[INFO] Ctrl+C para detener | Tópico único, diferenciado por message_type');
