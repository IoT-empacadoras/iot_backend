/**
 * Simulador de Panel View Xinje HMI
 * 
 * Este script simula el envío de datos desde un panel view Xinje HMI
 * al broker MQTT para propósitos de testing y desarrollo.
 */

const mqtt = require('mqtt');

// Configuración del broker MQTT (ajustar según tu .env)
const BROKER_URL = 'mqtt://broker.hivemq.com:1883';
const DEVICE_ID = '441095104B78F267112345678';
const TOPIC = `${DEVICE_ID}/pub_data`;

// Rango de valores para simulación
const CONFIG = {
  cantidad_productos: { min: 0, max: 500 },
  temperatura: { min: 18.0, max: 35.0 }
};

// Estado actual de las variables
let currentData = {
  cantidad_productos: 150,
  temperatura: 24.5
};

// Conectar al broker
console.log(`[SIMULADOR] Conectando al broker MQTT: ${BROKER_URL}`);
const client = mqtt.connect(BROKER_URL, {
  clientId: 'hmi_simulator_' + Math.random().toString(16).substr(2, 8),
  clean: true
});

client.on('connect', () => {
  console.log('[SIMULADOR] Conectado al broker exitosamente');
  console.log(`[SIMULADOR] Publicando en topic: ${TOPIC}`);
  console.log('[SIMULADOR] Iniciando simulacion de datos...\n');
  
  // Publicar datos cada 10 segundos
  setInterval(() => {
    publishData();
  }, 10000);
  
  // Primera publicación inmediata
  publishData();
});

client.on('error', (error) => {
  console.error('[ERROR] Error de conexion:', error.message);
  process.exit(1);
});

client.on('close', () => {
  console.log('[SIMULADOR] Desconectado del broker');
});

/**
 * Generar variación aleatoria en los datos
 */
function updateSimulatedData() {
  // Cantidad de productos: incrementos/decrementos de 0-20
  const cantidadChange = Math.floor(Math.random() * 40) - 20;
  currentData.cantidad_productos = Math.max(
    CONFIG.cantidad_productos.min,
    Math.min(CONFIG.cantidad_productos.max, currentData.cantidad_productos + cantidadChange)
  );
  
  // Temperatura: variaciones de -1.0 a +1.0 grados
  const tempChange = (Math.random() * 2 - 1).toFixed(1);
  currentData.temperatura = Math.max(
    CONFIG.temperatura.min,
    Math.min(CONFIG.temperatura.max, parseFloat((currentData.temperatura + parseFloat(tempChange)).toFixed(1)))
  );
}

/**
 * Publicar datos al broker MQTT en formato Xinje
 */
function publishData() {
  // Actualizar valores simulados
  updateSimulatedData();
  
  // Construir payload según formato Xinje HMI
  const payload = {
    Variant: [{
      Unix: Date.now().toString(),
      Version: "V1.0",
      Pub_Data: {
        "Nombre_Dispositivo": {
          cantidad_productos: currentData.cantidad_productos,
          temperatura: currentData.temperatura
        }
      }
    }]
  };
  
  const message = JSON.stringify(payload);
  
  client.publish(TOPIC, message, { qos: 2 }, (err) => {
    if (err) {
      console.error('[ERROR] Error al publicar:', err);
    } else {
      const timestamp = new Date().toISOString();
      console.log(`[${timestamp}] Datos publicados:`);
      console.log(`   - Cantidad productos: ${currentData.cantidad_productos}`);
      console.log(`   - Temperatura: ${currentData.temperatura} C\n`);
    }
  });
}

// Manejar cierre gracioso
process.on('SIGINT', () => {
  console.log('\n[SIMULADOR] Deteniendo simulador...');
  client.end(() => {
    console.log('[SIMULADOR] Simulador detenido');
    process.exit(0);
  });
});

console.log('[INFO] Presiona Ctrl+C para detener el simulador');
