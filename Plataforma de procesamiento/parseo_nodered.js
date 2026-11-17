// ============================================================
//  Parseo Node-RED — Sitio A (Invernadero) + Sitio B (Exterior)
//  Proyecto Final Ingeniería Electrónica — GPON IoT
//
//  Tópicos Sitio A:
//    sitioA/dht11              → {"temperatura":25.4,"humedad":60.1}
//    sitioA/h_suelo            → {"raw":2800,"porcentaje":32}
//    sitioA/luminosidad        → {"lux":740}
//    sitioA/gas                → {"nivel":1200}
//    sitioA/lm35               → {"temperatura_suelo":6.4}
//    sitioA/eventos/temp       → {"estado":"ALERTA","valor":36.2}
//    sitioA/eventos/riego      → {"estado":"RIEGO_ACTIVO","valor":3100.0}
//    sitioA/eventos/gas        → {"estado":"ALARMA","valor":2800.0}
//    sitioA/eventos/helada     → {"estado":"HELADA","valor":2.3}
//    sitioA/sistema/reinicio   → {"motivo":"INT_WDT","codigo":5}
//
//  Tópicos Sitio B:
//    sitioB/dht11              → {"temperatura":18.2,"humedad":72.0}
//    sitioB/lluvia             → {"raw":800,"lluvia":true}
//    sitioB/luminosidad        → {"lux":920}
//    sitioB/mpu6050            → {"accelX":0.12,"accelY":0.08,"accelZ":0.98,"vibracion":false}
//    sitioB/inclinacion        → {"inclinado":false}
//    sitioB/lm35               → {"temperatura_suelo":6.4}
//    sitioB/eventos/lluvia     → {"estado":"LLUVIA_ACTIVA","valor":800.0}
//    sitioB/eventos/viento     → {"estado":"VIENTO_FUERTE","valor":2.1}
//    sitioB/eventos/helada     → {"estado":"HELADA","valor":2.3}
//    sitioB/eventos/tormenta   → {"estado":"TORMENTA","valor":1.0}
//    sitioB/sistema/reinicio   → {"motivo":"INT_WDT","codigo":5}
// ============================================================

// Ignorar mensajes retenidos: el broker los reenvia al suscribirse
// (por ejemplo al reiniciar Node-RED) y no son eventos nuevos. Los mensajes
// publicados en vivo llegan con retain = false, asi que no se pierden.
if (msg.retain === true) {
    return null;
}

const partes  = msg.topic.split("/");
const sitio   = partes[0] || "desconocido";  // "sitioA" o "sitioB"
const tipo    = partes[1] || "desconocido";  // sensor o "eventos"
const subtipo = partes[2] || "";             // solo para eventos

// Parsear payload — puede llegar como string JSON u objeto
let data;
try {
    data = (typeof msg.payload === "string")
        ? JSON.parse(msg.payload)
        : msg.payload;
} catch(e) {
    node.warn("Payload inválido en tópico " + msg.topic + ": " + msg.payload);
    return null;
}

let measurement;

// ============================================================
//  SITIO A — Invernadero
// ============================================================
if (sitio === "sitioA") {

    switch(tipo) {

        case "dht11":
            measurement = {
                measurement: "temperatura_humedad",
                tags:   { sitio, sensor: "DHT11" },
                fields: {
                    temperatura: parseFloat(data.temperatura) || 0,
                    humedad:     parseFloat(data.humedad)     || 0
                }
            };
            break;

        case "h_suelo":
            measurement = {
                measurement: "humedad_suelo",
                tags:   { sitio, sensor: "FC-28" },
                fields: {
                    raw:        parseInt(data.raw)          || 0,
                    porcentaje: parseFloat(data.porcentaje) || 0
                }
            };
            break;

        case "luminosidad":
            measurement = {
                measurement: "luminosidad",
                tags:   { sitio, sensor: "LDR" },
                fields: {
                    lux: parseInt(data.lux) || 0
                }
            };
            break;

        case "gas":
            measurement = {
                measurement: "gas_humo",
                tags:   { sitio, sensor: "MQ2" },
                fields: {
                    nivel: parseInt(data.nivel) || 0
                }
            };
            break;
        
        case "lm35":
            measurement = {
                measurement: "temperatura_suelo",
                tags:   { sitio, sensor: "LM35" },
                fields: {
                    temperatura_suelo: parseFloat(data.temperatura_suelo) || 0
                }
            };
            break;

        case "eventos":
            measurement = {
                measurement: "eventos",
                tags:   { sitio, tipo_evento: subtipo },
                fields: {
                    estado: String(data.estado || "DESCONOCIDO"),
                    valor:  parseFloat(data.valor) || 0,
                    //Cuando el estado está OK, activo se pone en 0
                    activo: (data.estado !== "OK") ? 1 : 0
                }
            };
            break;

        case "sistema":
            // sitioX/sistema/reinicio -> {"motivo":"INT_WDT","codigo":5}
            measurement = {
                measurement: "reinicios",
                tags:   { sitio, sensor: "ESP32" },
                fields: {
                    motivo: String(data.motivo || "DESCONOCIDO"),
                    codigo: parseInt(data.codigo) || 0
                }
            };
            break;

        default:
            node.warn("[SitioA] Tópico no reconocido: " + msg.topic);
            return null;
    }
}

// ============================================================
//  SITIO B — Estación Meteorológica Exterior
// ============================================================
else if (sitio === "sitioB") {

    switch(tipo) {

        case "dht11":
            measurement = {
                measurement: "temperatura_humedad",
                tags:   { sitio, sensor: "DHT11" },
                fields: {
                    temperatura: parseFloat(data.temperatura) || 0,
                    humedad:     parseFloat(data.humedad)     || 0
                }
            };
            break;

        case "lluvia":
            measurement = {
                measurement: "lluvia",
                tags:   { sitio, sensor: "FC-37" },
                fields: {
                    raw:    parseInt(data.raw) || 0,
                    lluvia: (data.lluvia === true || data.lluvia === "true") ? 1 : 0
                }
            };
            break;

        case "luminosidad":
            measurement = {
                measurement: "luminosidad",
                tags:   { sitio, sensor: "LDR" },
                fields: {
                    lux: parseInt(data.lux) || 0
                }
            };
            break;

        case "mpu6050":
            measurement = {
                measurement: "vibracion_viento",
                tags:   { sitio, sensor: "MPU6050" },
                fields: {
                    accelX:    parseFloat(data.accelX) || 0,
                    accelY:    parseFloat(data.accelY) || 0,
                    accelZ:    parseFloat(data.accelZ) || 0,
                    vibracion: (data.vibracion === true || data.vibracion === "true") ? 1 : 0
                }
            };
            break;

        /*case "inclinacion":
            measurement = {
                measurement: "inclinacion",
                tags:   { sitio, sensor: "SensorInclinacion" },
                fields: {
                    inclinado: (data.inclinado === true || data.inclinado === "true") ? 1 : 0
                }
            };
            break;*/

        case "lm35":
            measurement = {
                measurement: "temperatura_suelo",
                tags:   { sitio, sensor: "LM35" },
                fields: {
                    temperatura_suelo: parseFloat(data.temperatura_suelo) || 0
                }
            };
            break;

        case "eventos":
            measurement = {
                measurement: "eventos",
                tags:   { sitio, tipo_evento: subtipo },
                fields: {
                    estado: String(data.estado || "DESCONOCIDO"),
                    valor:  parseFloat(data.valor) || 0,
                    //Cuando el estado está OK, activo se pone en 0
                    activo: (data.estado !== "OK") ? 1 : 0
                }
            };
            break;

        case "sistema":
            // sitioX/sistema/reinicio -> {"motivo":"INT_WDT","codigo":5}
            measurement = {
                measurement: "reinicios",
                tags:   { sitio, sensor: "ESP32" },
                fields: {
                    motivo: String(data.motivo || "DESCONOCIDO"),
                    codigo: parseInt(data.codigo) || 0
                }
            };
            break;

        default:
            node.warn("[SitioB] Tópico no reconocido: " + msg.topic);
            return null;
    }
}

// Sitio desconocido
else {
    node.warn("Sitio no reconocido: " + sitio);
    return null;
}

// ── Formato para nodo influxdb out v0.7.0 + InfluxDB 1.x ────
msg.payload = [{
    "measurement":  measurement.measurement,
    "sitio":        measurement.tags.sitio        || "",
    "sensor":       measurement.tags.sensor       || "",
    "tipo_evento":  measurement.tags.tipo_evento  || "",
    ...measurement.fields
}];

msg.measurement = measurement.measurement;

node.log(`[InfluxDB] ${sitio}/${tipo} → ${measurement.measurement} | ${JSON.stringify(measurement.fields)}`);

return msg;