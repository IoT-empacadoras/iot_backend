# Integración MQTT HMI TS5 (Xinje TouchWin Pro)

## Parámetros de Conexión
- **Broker:** broker.hivemq.com
- **Puerto:** 1883 (sin TLS)
- **Client ID:** Formato recomendado: `ID_del_dispositivo+Contraseña+Userdata`

## Estructura de Tópicos
- **Recibir datos (suscripción):** `ID+PWD/pub_data`
- **Controlar HMI (publicación):** `ID+PWD/write_data`
- **Consultar datos históricos:** `ID+PWD/access_data`
- **Respuesta a comandos:** `ID+PWD/write_reply`

## Ejemplo de Mensaje Recibido (`pub_data`)
```json
{
  "Variant": [{
    "Unix": "1614576888000",
    "Version": "V1.0",
    "Pub_Data": {
      "Nombre_del_Dispositivo": {
        "variable1": 23,
        "variable2": 50.23
      }
    }
  }]
}
```

## Ejemplo de Comando a HMI (`write_data`)
```json
{
  "Unix": "1614576888000",
  "Version": "V1.0",
  "Write_Data": {
    "Nombre_del_Dispositivo": {
      "variable_a_cambiar": 20
    }
  }
}
```

## Ejemplo de Consulta de Datos Históricos (`access_data`)
```json
{
  "Access_Data": {
    "Nombre_del_Dispositivo": {
      "variable": "nombre_variable",
      "start": 1614576888000,
      "end": 1614577899000
    }
  }
}
```

## Consideraciones de Seguridad
- La contraseña remota por defecto es `12345678` si no se ha cambiado.
- El uso de broker público implica que los tópicos y credenciales viajan en texto plano.
