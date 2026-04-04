const path = require('path');

require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const {
  initConnectionPool,
  initDatabase,
  activatePpmProfile,
  getSensors
} = require('../src/database');

async function main() {
  const deviceId = process.argv[2];

  if (!deviceId) {
    console.error('Uso: node backend/scripts/set_active_ppm.js <DEVICE_ID>');
    process.exit(1);
  }

  try {
    await initConnectionPool();
    await initDatabase();

    const result = await activatePpmProfile(deviceId);
    const activeSensors = await getSensors(deviceId);

    console.log('[OK] Perfil PPM aplicado');
    console.log('deviceId:', deviceId);
    console.log('totalActive:', result.totalActive);
    console.log('tagsActivos:', activeSensors.map((s) => s.tag_name));
    process.exit(0);
  } catch (error) {
    console.error('[ERROR] No se pudo aplicar perfil PPM:', error.message);
    process.exit(1);
  }
}

main();
