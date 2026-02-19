-- ================================================================
-- MySQL Event Scheduler para Agregación de Datos de Sensores
-- ================================================================
-- Este script configura eventos automáticos que calculan
-- promedios, mínimos y máximos en intervalos de tiempo.
--
-- Requisitos:
-- 1. MySQL 5.1+ con Event Scheduler habilitado
-- 2. Base de datos xinje_iot creada
-- 3. Tablas sensor_history y agregados creados
--
-- Para ejecutar:
-- mysql -u root -p xinje_iot < mysql_events.sql
-- ================================================================

USE xinje_iot;

-- Habilitar Event Scheduler (si no está habilitado)
SET GLOBAL event_scheduler = ON;

-- ================================================================
-- EVENTO 1: Agregación por 1 MINUTO
-- ================================================================
-- Se ejecuta cada 1 minuto
-- Calcula promedios del último minuto completo

DROP EVENT IF EXISTS aggregate_sensor_data_1min;

DELIMITER $$
CREATE EVENT aggregate_sensor_data_1min
ON SCHEDULE EVERY 1 MINUTE
STARTS CURRENT_TIMESTAMP
DO
BEGIN
  INSERT INTO sensor_history_1min (sensor_id, minute_ts, avg_value, min_value, max_value, count)
  SELECT 
    sensor_id,
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') as minute_ts,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as count
  FROM sensor_history
  WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 2 MINUTE)
    AND timestamp < DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:00')
  GROUP BY sensor_id, minute_ts
  ON DUPLICATE KEY UPDATE
    avg_value = VALUES(avg_value),
    min_value = VALUES(min_value),
    max_value = VALUES(max_value),
    count = VALUES(count);
END$$
DELIMITER ;

-- ================================================================
-- EVENTO 2: Agregación por 5 MINUTOS
-- ================================================================
-- Se ejecuta cada 5 minutos
-- Calcula promedios de los últimos 5 minutos completos

DROP EVENT IF EXISTS aggregate_sensor_data_5min;

DELIMITER $$
CREATE EVENT aggregate_sensor_data_5min
ON SCHEDULE EVERY 5 MINUTE
STARTS CURRENT_TIMESTAMP
DO
BEGIN
  INSERT INTO sensor_history_5min (sensor_id, interval_ts, avg_value, min_value, max_value, count)
  SELECT 
    sensor_id,
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / 300) * 300) as interval_ts,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as count
  FROM sensor_history
  WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)
    AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW()) / 300) * 300)
  GROUP BY sensor_id, interval_ts
  ON DUPLICATE KEY UPDATE
    avg_value = VALUES(avg_value),
    min_value = VALUES(min_value),
    max_value = VALUES(max_value),
    count = VALUES(count);
END$$
DELIMITER ;

-- ================================================================
-- EVENTO 3: Agregación por 10 MINUTOS
-- ================================================================
-- Se ejecuta cada 10 minutos
-- Calcula promedios de los últimos 10 minutos completos

DROP EVENT IF EXISTS aggregate_sensor_data_10min;

DELIMITER $$
CREATE EVENT aggregate_sensor_data_10min
ON SCHEDULE EVERY 10 MINUTE
STARTS CURRENT_TIMESTAMP
DO
BEGIN
  INSERT INTO sensor_history_10min (sensor_id, interval_ts, avg_value, min_value, max_value, count)
  SELECT 
    sensor_id,
    FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(timestamp) / 600) * 600) as interval_ts,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as count
  FROM sensor_history
  WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 20 MINUTE)
    AND timestamp < FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(NOW()) / 600) * 600)
  GROUP BY sensor_id, interval_ts
  ON DUPLICATE KEY UPDATE
    avg_value = VALUES(avg_value),
    min_value = VALUES(min_value),
    max_value = VALUES(max_value),
    count = VALUES(count);
END$$
DELIMITER ;

-- ================================================================
-- EVENTO 4: Agregación por 1 HORA
-- ================================================================
-- Se ejecuta cada 1 hora
-- Calcula promedios de la última hora completa

DROP EVENT IF EXISTS aggregate_sensor_data_1hour;

DELIMITER $$
CREATE EVENT aggregate_sensor_data_1hour
ON SCHEDULE EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP
DO
BEGIN
  INSERT INTO sensor_history_1hour (sensor_id, hour_ts, avg_value, min_value, max_value, count)
  SELECT 
    sensor_id,
    DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00') as hour_ts,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    COUNT(*) as count
  FROM sensor_history
  WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
    AND timestamp < DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00')
  GROUP BY sensor_id, hour_ts
  ON DUPLICATE KEY UPDATE
    avg_value = VALUES(avg_value),
    min_value = VALUES(min_value),
    max_value = VALUES(max_value),
    count = VALUES(count);
END$$
DELIMITER ;

-- ================================================================
-- VERIFICACIÓN DE EVENTOS
-- ================================================================
-- Verificar que los eventos están creados y activos

SELECT 
  event_name,
  event_definition,
  interval_value,
  interval_field,
  status,
  last_executed,
  next_execution_time
FROM information_schema.EVENTS
WHERE event_schema = 'xinje_iot'
ORDER BY event_name;

-- ================================================================
-- COMANDOS ÚTILES
-- ================================================================

-- Verificar si el Event Scheduler está habilitado:
-- SHOW VARIABLES LIKE 'event_scheduler';

-- Habilitar Event Scheduler (requiere permisos SUPER):
-- SET GLOBAL event_scheduler = ON;

-- Ver todos los eventos:
-- SHOW EVENTS FROM xinje_iot;

-- Pausar un evento:
-- ALTER EVENT aggregate_sensor_data_1min DISABLE;

-- Reanudar un evento:
-- ALTER EVENT aggregate_sensor_data_1min ENABLE;

-- Eliminar un evento:
-- DROP EVENT IF EXISTS aggregate_sensor_data_1min;

-- Ver cuántos datos se han agregado:
-- SELECT COUNT(*) FROM sensor_history_1min;
-- SELECT COUNT(*) FROM sensor_history_5min;
-- SELECT COUNT(*) FROM sensor_history_10min;
-- SELECT COUNT(*) FROM sensor_history_1hour;

-- Ver últimos agregados de 1 minuto:
-- SELECT s.tag_name, h.minute_ts, h.avg_value, h.min_value, h.max_value, h.count
-- FROM sensor_history_1min h
-- JOIN sensors s ON h.sensor_id = s.sensor_id
-- ORDER BY h.minute_ts DESC
-- LIMIT 20;
