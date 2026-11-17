// ============================================================
//  Sitio B — Estación Meteorológica Exterior
//  Sistema de telemetría sobre infraestructura de red GPON
//  Proyecto Final Ingeniería Electrónica
//
//  Sensores:
//    - DHT11          → temperatura y humedad exterior
//    - FC-37          → detección de lluvia activa
//    - LDR            → luminosidad exterior
//    - MPU6050        → vibración por viento / ráfagas
//    - LM35           → temperatura del suelo exterior
//
//  Actuadores (LEDs):
//    - LED_LLUVIA  (26) → lluvia activa detectada
//    - LED_VIENTO  (27) → vibración/viento fuerte
//    - LED_HELADA  (14) → temperatura del suelo < umbral helada
//    - LED_TORMENTA(12) → lluvia + viento simultáneos
//
//  Buses I2C (el ESP32 tiene 2 controladores independientes):
//    - Wire  (I2C0): MPU6050 → SDA GPIO 18, SCL GPIO 19, dirección 0x68
//    - Wire1 (I2C1): OLED    → SDA GPIO 21, SCL GPIO 22, dirección 0x3C
//                              (algunos módulos usan 0x3D)
//    - Librerías OLED: "Adafruit SSD1306" y "Adafruit GFX Library"
//
//  Tópicos MQTT publicados:
//    sitioB/dht11              → {"temperatura":18.2,"humedad":72.0}
//    sitioB/lluvia             → {"raw":800,"lluvia":true}
//    sitioB/luminosidad        → {"lux":920}
//    sitioB/mpu6050            → {"accelX":0.12,"accelY":0.08,"accelZ":0.98,"vibracion":false}
//    sitioB/lm35               → {"temperatura_suelo":6.4}
//    sitioB/eventos/lluvia     → {"estado":"LLUVIA_ACTIVA","valor":800.0}
//                                {"estado":"OK","valor":0.0}
//    sitioB/eventos/viento     → {"estado":"VIENTO_FUERTE","valor":2.1}
//                                {"estado":"OK","valor":0.0}
//    sitioB/eventos/helada     → {"estado":"HELADA","valor":2.3}
//                                {"estado":"OK","valor":0.0}
//    sitioB/eventos/tormenta   → {"estado":"TORMENTA","valor":1.0}
//                                {"estado":"OK","valor":0.0}
//    sitioB/sistema/reinicio   → {"motivo": "POWERON", "codigo": 1}
// ============================================================

#include <Arduino.h>
#include <DHT.h>
#include <Wire.h>
#include <MPU6050.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

// ── Configuración de red ─────────────────────────────────────
#define WIFI_SSID        "MQTT-SitioB"
#define WIFI_PASSWORD    "*****"
#define MQTT_BROKER      "192.168.20.198"
#define MQTT_PORT        1883
#define MQTT_CLIENT_ID   "ESP32_SitioB"

// ── Pines GPIO ───────────────────────────────────────────────
#define PIN_DHT          4    // DHT11 data
#define PIN_LLUVIA       34   // Sensor lluvia analógico (ADC1)
#define PIN_LDR          32   // Fotoresistencia (ADC1)
#define PIN_LM35         36   // LM35 temperatura suelo (ADC1)

#define LED_LLUVIA       26   // lluvia activa
#define LED_VIENTO       27   // viento fuerte
#define LED_HELADA       14   // helada inminente
#define LED_TORMENTA     12   // tormenta (lluvia + viento)

// ── Buses I2C y display OLED ─────────────────────────────────
#define PIN_MPU_SDA      18   // Wire  (I2C0) — solo MPU6050
#define PIN_MPU_SCL      19
#define PIN_OLED_SDA     21   // Wire1 (I2C1) — solo OLED
#define PIN_OLED_SCL     22
#define OLED_ADDR        0x3C // probar 0x3D si no inicializa
#define OLED_ANCHO       128
#define OLED_ALTO        64
#define OLED_RESET       -1   // el módulo de 4 pines no tiene pin RESET

#define INTERVALO_OLED   500  // ms — refresco de pantalla
#define OLED_CICLOS_PAG  6    // refrescos por página (6 × 500 ms = 3 s)
#define OLED_NUM_PAGINAS 6

// ── Umbrales de alerta ───────────────────────────────────────
#define UMBRAL_LLUVIA       2500    // ADC — debajo de este valor hay lluvia
#define UMBRAL_VIBRACION    1.2f    // g   — viento fuerte, eje X o Y
#define UMBRAL_HELADA      5.0f    // °C suelo — riesgo de helada
#define RETENCION_VIENTO  2000     // ms  — el viento sigue "fuerte" hasta pasar este tiempo sin superar el umbral
// #define UMBRAL_LUZ_MIN   100     // umbral de luz en caso de necesitarse

// ── Validez de lectura DHT11 ─────────────────────────────────
#define DHT_LEC_VAL_MS  30000    // ms — lectura válida por máximo 30 segundos

// ── Intervalos de muestreo ───────────────────────────────────
#define INTERVALO_LENTOS    5000    // ms — DHT11, lluvia, luz, LM35
#define INTERVALO_MPU        100    // ms — acelerómetro (muestreo rápido para ráfagas)
#define INTERVALO_MQTT      5000    // ms — publicación periódica de telemetría

// ── Cola MQTT ────────────────────────────────────────────────
// Todos los mensajes MQTT se encolan y se publican desde taskMQTT (Core 1)
// Esto evita accesos concurrentes al cliente PubSubClient
#define MQTT_QUEUE_SIZE  20       // máximo mensajes en cola simultáneos
#define MQTT_TOPIC_LEN   64       // longitud máxima del tópico
#define MQTT_PAYLOAD_LEN 128      // longitud máxima del payload

// ── Offset humedad ────────────────────────────────────────────

#define OFFSET_HUMEDAD   32.0f

struct MqttMessage {
    char topic[MQTT_TOPIC_LEN];
    char payload[MQTT_PAYLOAD_LEN];
    bool retain;
};

QueueHandle_t mqttQueue;

// ── Variables compartidas entre tareas ───────────────────────
volatile float  t_dht        = 0.0f;
volatile float  h_dht        = 0.0f;
volatile int    lluvia       = 0;
volatile int    lux          = 0;
volatile float  t_suelo      = 0.0f;
volatile float  accelX       = 0.0f;
volatile float  accelY       = 0.0f;
volatile float  accelZ       = 0.0f;
volatile bool   vientoFuerte = false;

// Validez del DHT11
volatile bool     dhtValido    = false;
volatile uint32_t dhtUltimoms  = 0;

// Estado de alarmas (escritas por las tareas de sensores, leídas por el display)
// El viento usa la variable vientoFuerte ya existente
volatile bool   alarmaLluvia   = false;
volatile bool   alarmaHelada   = false;
volatile bool   alarmaTormenta = false;

// Estado del MPU6050 (false si no respondió al iniciar)
volatile bool   mpuOK          = false;

// Estado MQTT (escrito solo por taskMQTT, para no tocar PubSubClient
// desde otra tarea). mqttEstado guarda mqtt.state():
//   0 = conectado, -1 = desconectado, -2 = fallo de conexión,
//  -4 = timeout, 5 = no autorizado, etc.
volatile int    mqttEstado     = -1;
volatile bool   mqttConectado  = false;
// Sincronizacion de eventos: cada conexion MQTT exitosa incrementa la epoca;
// cada tarea republica el estado actual de sus eventos al detectar una epoca nueva
// (evita que un evento quede "activo" en Grafana si el nodo se reinicia o pierde la conexion)
volatile uint32_t epocaMQTT = 0;
// Motivo del ultimo reinicio: se publica una sola vez por arranque, en sitioX/sistema/reinicio
char motivoResetTxt[16] = "OTRO";
int  codigoReset = 0;
bool resetPublicado = false;


// ── Objetos globales ─────────────────────────────────────────
DHT              dht(PIN_DHT, DHT11);
MPU6050          mpu;
WiFiClient       espClient;
PubSubClient     mqtt(espClient);
Adafruit_SSD1306 oled(OLED_ANCHO, OLED_ALTO, &Wire1, OLED_RESET);  // bus propio

// ── Prototipos ───────────────────────────────────────────────
// Tareas
void taskSensoresLentos(void* pvParameters);
void taskMPU6050(void* pvParameters);
void taskMQTT(void* pvParameters);
void taskWiFiWatchdog(void* pvParameters);
void taskDisplay(void* pvParameters);

// Display OLED
void oledIconoWiFi(int x, int y);
void oledEtiquetaMQTT(int x, int y);
void oledBarraEstado(int pagina, bool parpadeo);
void oledGradosC();
void oledBarraAccel(int x, int y, int ancho, float valor_g);
void oledPaginaAire();
void oledPaginaLluviaLuz();
void oledPaginaViento();
void oledPaginaSuelo();
void oledPaginaAlarmas();
void oledPaginaRed();

// MQTT, red y sistema
void encolarMQTT(const char* topico, const char* payload, bool retain = false);
void encolarEvento(const char* topico, const char* estado, float valor);
void connectWiFi();
void connectMQTT();
void imprimirMotivoReset();

// Conversión de lecturas
float adcToLM35(int raw);
int   adcToLux(int raw);

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    delay(1000);
    Serial.println("\n=== Sitio B — Estación Meteorológica Exterior ===");
    imprimirMotivoReset();

    // ── Pines de salida (LEDs) ──
    pinMode(LED_LLUVIA,   OUTPUT);
    pinMode(LED_VIENTO,   OUTPUT);
    pinMode(LED_HELADA,   OUTPUT);
    pinMode(LED_TORMENTA, OUTPUT);

    digitalWrite(LED_LLUVIA,   LOW);
    digitalWrite(LED_VIENTO,   LOW);
    digitalWrite(LED_HELADA,   LOW);
    digitalWrite(LED_TORMENTA, LOW);

    // ── Display OLED (Wire1, bus propio) ──
    Wire1.begin(PIN_OLED_SDA, PIN_OLED_SCL);
    Wire1.setClock(400000);             // I2C rápido: refresco ~25 ms
   
    bool oledOK = oled.begin(SSD1306_SWITCHCAPVCC, OLED_ADDR, true, false);
    if (oledOK) {
        oled.cp437(true);               // habilita el símbolo ° (carácter 247)
        oled.clearDisplay();
        oled.setTextColor(SSD1306_WHITE);
        oled.setTextSize(2);
        oled.setCursor(16, 8);
        oled.print("SITIO B");
        oled.setTextSize(1);
        oled.setCursor(10, 32);
        oled.print("Estacion Exterior");
        oled.setCursor(22, 48);
        oled.print("Iniciando...");
        oled.display();
        Serial.println("[INIT] OLED OK");
    } else {
        // El sistema sigue funcionando sin display
        Serial.println("[ERROR] OLED no encontrado (revisar cableado / dirección I2C)");
    }

    // ── DHT11 ──
    dht.begin();
    Serial.println("[INIT] DHT11 OK");

    // ── MPU6050 (Wire, bus propio) ──
    Wire.begin(PIN_MPU_SDA, PIN_MPU_SCL);
    mpu.initialize();
    if (mpu.testConnection()) {
        mpuOK = true;
        Serial.println("[INIT] MPU6050 OK");
    } else {
        Serial.println("[ERROR] MPU6050 no encontrado. Verificá I2C SDA=18 SCL=19");
    }

    // ── Cola MQTT ──
    mqttQueue = xQueueCreate(MQTT_QUEUE_SIZE, sizeof(MqttMessage));
    if (mqttQueue == NULL) {
        Serial.println("[ERROR] No se pudo crear la cola MQTT. Reiniciando...");
        ESP.restart();
    }
    Serial.println("[INIT] Cola MQTT OK");

    // ── WiFi + MQTT ──
    connectWiFi();
    mqtt.setServer(MQTT_BROKER, MQTT_PORT);
    mqtt.setBufferSize(512);
    connectMQTT();
    mqttConectado = mqtt.connected();   // estado inicial para el display

    // ── Tareas FreeRTOS ──
    //  Core 0: sensores rápidos
    //  Core 1: sensores lentos + red (WiFi + MQTT corren junto al stack TCP/IP) y display
    xTaskCreatePinnedToCore(taskSensoresLentos, "SensLentos", 4096, NULL, 1, NULL, 1);
    xTaskCreatePinnedToCore(taskMPU6050,        "MPU6050",    4096, NULL, 3, NULL, 0);
    xTaskCreatePinnedToCore(taskMQTT,           "MQTT_Pub",   8192, NULL, 2, NULL, 1);
    xTaskCreatePinnedToCore(taskWiFiWatchdog,   "WiFi_WD",    4096, NULL, 1, NULL, 1);
    if (oledOK) {
        xTaskCreatePinnedToCore(taskDisplay,    "Display",    6144, NULL, 1, NULL, 1);
    }

    Serial.println("[INIT] Tareas iniciadas. FreeRTOS corriendo.");
}

// ── LOOP vacío — FreeRTOS toma el control ───────────────────
void loop() {
    vTaskDelay(portMAX_DELAY);
}

// ============================================================
//  TAREA 1: Sensores lentos — Core 0
//  DHT11, lluvia, fotoresistencia, LM35
//  Frecuencia: cada 5 segundos
// ============================================================
void taskSensoresLentos(void* pvParameters) {
    Serial.println("[TASK] SensoresLentos iniciada");

    // Flags de estado para publicar solo en transición
    bool lluviaAct   = false;
    bool heladaAct   = false;
    bool tormentaAct = false;
    uint32_t epocaLocal = 0;       // ultima epoca MQTT sincronizada

    for (;;) {
        // Si hubo una conexion MQTT nueva (o recien arranco), republicar el estado actual
        bool resync = (epocaMQTT != epocaLocal);
        epocaLocal = epocaMQTT;

        // ── DHT11 exterior ──────────────────────────────────
        float tLeida = dht.readTemperature();
        float hLeida = dht.readHumidity() + OFFSET_HUMEDAD;
        hLeida = constrain(hLeida, 0.0f, 100.0f);   // limita a 0-100 % tras el offset (NaN se conserva)

        if (!isnan(tLeida) && !isnan(hLeida)) {
            t_dht       = tLeida;
            h_dht       = hLeida;
            dhtValido   = true;
            dhtUltimoms = millis();
        } else {
            Serial.println("[WARN] DHT11: lectura inválida");
            // Si la última lectura válida supera DHT_LEC_VAL_MS, marcar inválido
            if (millis() - dhtUltimoms > DHT_LEC_VAL_MS) {
                dhtValido = false;
                Serial.println("[WARN] DHT11: dato expirado, no se evalúan alarmas");
            }
        }

        // ── Sensor de lluvia (promedio 5 lecturas) ───────────
        int sumaLluvia = 0;
        for (int i = 0; i < 5; i++) {
            sumaLluvia += analogRead(PIN_LLUVIA);
            vTaskDelay(20 / portTICK_PERIOD_MS);
        }
        lluvia = sumaLluvia / 5;
        bool hayLluvia = (lluvia < UMBRAL_LLUVIA);

        // ── Fotoresistencia (promedio 5 lecturas) ────────────
        int sumaLDR = 0;
        for (int i = 0; i < 5; i++) {
            sumaLDR += analogRead(PIN_LDR);
            vTaskDelay(20 / portTICK_PERIOD_MS);
        }
        lux = adcToLux(sumaLDR / 5);

        // ── LM35 temperatura suelo (promedio 10 lecturas) ────
        int sumaLM35 = 0;
        for (int i = 0; i < 10; i++) {
            sumaLM35 += analogRead(PIN_LM35);
            vTaskDelay(10 / portTICK_PERIOD_MS);
        }
        t_suelo = adcToLM35(sumaLM35 / 10);

        // ── Evaluar alarmas y LEDs ───────────────────────────

        // Lluvia activa
        alarmaLluvia = hayLluvia;
        digitalWrite(LED_LLUVIA, hayLluvia ? HIGH : LOW);
        if (hayLluvia && (!lluviaAct || resync)) {
            lluviaAct = true;
            encolarEvento("sitioB/eventos/lluvia", "LLUVIA_ACTIVA", lluvia);
        } else if (!hayLluvia && (lluviaAct || resync)) {
            lluviaAct = false;
            encolarEvento("sitioB/eventos/lluvia", "OK", 0.0f);
        }

        // Helada (temperatura del suelo)
        bool heladaRiesgo = (t_suelo < UMBRAL_HELADA);
        alarmaHelada = heladaRiesgo;
        digitalWrite(LED_HELADA, heladaRiesgo ? HIGH : LOW);
        if (heladaRiesgo && (!heladaAct || resync)) {
            heladaAct = true;
            encolarEvento("sitioB/eventos/helada", "HELADA", t_suelo);
        } else if (!heladaRiesgo && (heladaAct || resync)) {
            heladaAct = false;
            encolarEvento("sitioB/eventos/helada", "OK", 0.0f);
        }

        // Tormenta: lluvia + viento fuerte simultáneos
        bool tormenta = (hayLluvia && vientoFuerte);
        alarmaTormenta = tormenta;
        digitalWrite(LED_TORMENTA, tormenta ? HIGH : LOW);
        if (tormenta && (!tormentaAct || resync)) {
            tormentaAct = true;
            encolarEvento("sitioB/eventos/tormenta", "TORMENTA", 1.0f);
        } else if (!tormenta && (tormentaAct || resync)) {
            tormentaAct = false;
            encolarEvento("sitioB/eventos/tormenta", "OK", 0.0f);
        }

        Serial.printf("[SENSL] T:%.1f°C H:%.1f%% Lluvia:%d(%s) Luz:%d Tsuelo:%.1f°C DHT:%s\n",
            t_dht, h_dht, lluvia, hayLluvia ? "SI" : "NO",
            lux, t_suelo, dhtValido ? "OK" : "INVALIDO");

        vTaskDelay(INTERVALO_LENTOS / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 2: MPU6050 — Core 0
//  Muestreo rápido para detectar ráfagas de viento (100ms)
// ============================================================
void taskMPU6050(void* pvParameters) {
    Serial.println("[TASK] MPU6050 iniciada");

    bool vientoAct = false;
    uint32_t tUltimaRafaga = 0;   // instante de la última lectura sobre el umbral
    uint32_t epocaViento = 0;     // ultima epoca MQTT sincronizada

    for (;;) {
        int16_t ax, ay, az, gx, gy, gz;
        mpu.getMotion6(&ax, &ay, &az, &gx, &gy, &gz);   // Wire, bus exclusivo

        accelX = ax / 16384.0f;
        accelY = ay / 16384.0f;
        accelZ = az / 16384.0f;

        // Detecta vibración en ejes horizontales (X e Y)
        // Eje Z en reposo ≈ 1g (gravedad), no se evalúa para viento
        bool vibracionDetectada = (fabsf(accelX) > UMBRAL_VIBRACION ||
                                   fabsf(accelY) > UMBRAL_VIBRACION);

        // Retención: tras una ráfaga, el viento se mantiene "fuerte" hasta que
        // pasan RETENCION_VIENTO ms sin superar el umbral. Evita decenas de
        // eventos FUERTE/OK por segundo y permite que coincida con la lluvia.
        uint32_t ahora = millis();
        if (vibracionDetectada) tUltimaRafaga = ahora;
        bool vientoEstado = vibracionDetectada ||
                            (vientoAct && (ahora - tUltimaRafaga) < RETENCION_VIENTO);

        // Variable compartida leída por taskSensoresLentos (tormenta) y el display
        vientoFuerte = vientoEstado;

        bool resync = (epocaMQTT != epocaViento);
        epocaViento = epocaMQTT;

        // LED de viento controlado desde esta tarea (muestreo más rápido)
        digitalWrite(LED_VIENTO, vientoEstado ? HIGH : LOW);

        if (vientoEstado && (!vientoAct || resync)) {
            vientoAct = true;
            float magnitud = max(fabsf(accelX), fabsf(accelY));
            encolarEvento("sitioB/eventos/viento", "VIENTO_FUERTE", magnitud);
            Serial.printf("[ALARMA] Viento fuerte! X:%.2f Y:%.2f g\n", accelX, accelY);
        } else if (!vientoEstado && (vientoAct || resync)) {
            vientoAct = false;
            encolarEvento("sitioB/eventos/viento", "OK", 0.0f);
        }
        Serial.printf("[SENSL] Accelx:%.1f Accely:%.1f Accelz:%.1f\n",
            accelX, accelY, accelZ);

        vTaskDelay(INTERVALO_MPU / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 3: Publicación MQTT — Core 1
//  Publica telemetría periódica y desencola de eventos
// ============================================================
void taskMQTT(void* pvParameters) {
    Serial.println("[TASK] MQTT iniciada");

    for (;;) {
        if (!mqtt.connected()) {
            mqttConectado = false;
            connectMQTT();
        }
        mqtt.loop();

        // Publica estado para el display (solo esta tarea toca PubSubClient)
        mqttConectado = mqtt.connected();
        mqttEstado    = mqtt.state();

        if (mqtt.connected()) {
            // ── Motivo del último reinicio (una sola vez por arranque) ──
            if (!resetPublicado) {
                char rbuf[96];
                snprintf(rbuf, sizeof(rbuf), "{\"motivo\":\"%s\",\"codigo\":%d}", motivoResetTxt, codigoReset);
                if (mqtt.publish("sitioB/sistema/reinicio", rbuf)) {
                    resetPublicado = true;
                    Serial.printf("[MQTT] Reinicio publicado: %s\n", rbuf);
                }
            }

            // ── Telemetría periódica ─────────────────────────
            char buf[MQTT_PAYLOAD_LEN];

            // El DHT11 se publica solo si tiene una lectura válida reciente:
            // si falla o se desconecta, no publica nada.
            if (dhtValido) {
                snprintf(buf, sizeof(buf), "{\"temperatura\":%.1f,\"humedad\":%.1f}",
                    (float)t_dht, (float)h_dht);
                mqtt.publish("sitioB/dht11", buf);
            }

            bool hayLluvia = (lluvia < UMBRAL_LLUVIA);
            snprintf(buf, sizeof(buf), "{\"raw\":%d,\"lluvia\":%s}",
                lluvia, hayLluvia ? "true" : "false");
            mqtt.publish("sitioB/lluvia", buf);

            snprintf(buf, sizeof(buf), "{\"lux\":%d}", lux);
            mqtt.publish("sitioB/luminosidad", buf);

            snprintf(buf, sizeof(buf),
                "{\"accelX\":%.2f,\"accelY\":%.2f,\"accelZ\":%.2f,\"vibracion\":%s}",
                (float)accelX, (float)accelY, (float)accelZ,
                vientoFuerte ? "true" : "false");
            mqtt.publish("sitioB/mpu6050", buf);

            snprintf(buf, sizeof(buf), "{\"temperatura_suelo\":%.1f}", (float)t_suelo);
            mqtt.publish("sitioB/lm35", buf);

            Serial.println("[MQTT] Telemetría publicada");

            // ── Desencola eventos ─────────────────────
            MqttMessage msg;
            while (xQueueReceive(mqttQueue, &msg, 0) == pdTRUE) {
                mqtt.publish(msg.topic, msg.payload, msg.retain);
                Serial.printf("[MQTT] Evento: %s → %s\n", msg.topic, msg.payload);
            }
        }

        vTaskDelay(INTERVALO_MQTT / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 4: WiFi watchdog — Core 1
// ============================================================
void taskWiFiWatchdog(void* pvParameters) {
    for (;;) {
        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("[WIFI] Conexión perdida, reconectando...");
            WiFi.disconnect();
            connectWiFi();
        }
        vTaskDelay(10000 / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 5: Display OLED — Core 1
//  Barra de estado fija + páginas rotativas cada 3 s
//  Es la única tarea que usa Wire1 (bus exclusivo del OLED)
// ============================================================

void taskDisplay(void* pvParameters) {
    Serial.println("[TASK] Display OLED iniciada");

    int pagina = 0;
    int ciclos = 0;
    bool parpadeo = false;

    for (;;) {
        parpadeo = !parpadeo;

        oled.clearDisplay();
        oled.setTextSize(1);
        oled.setTextColor(SSD1306_WHITE);

        oledBarraEstado(pagina, parpadeo);

        switch (pagina) {
            case 0: oledPaginaAire();      break;
            case 1: oledPaginaSuelo();     break;
            case 2: oledPaginaLluviaLuz(); break;
            case 3: oledPaginaViento();    break;
            case 4: oledPaginaAlarmas();   break;
            case 5: oledPaginaRed();       break;
        }

        // Transferencia al display por Wire1 (~25 ms a 400 kHz)
        oled.display();

        if (++ciclos >= OLED_CICLOS_PAG) {
            ciclos = 0;
            pagina = (pagina + 1) % OLED_NUM_PAGINAS;
        }

        vTaskDelay(INTERVALO_OLED / portTICK_PERIOD_MS);
    }
}

// Ícono de señal WiFi: 4 barras según RSSI, o "x" si no hay conexión
void oledIconoWiFi(int x, int y) {
    bool conectado = (WiFi.status() == WL_CONNECTED);
    int barras = 0;
    if (conectado) {
        long rssi = WiFi.RSSI();
        if      (rssi > -55) barras = 4;
        else if (rssi > -65) barras = 3;
        else if (rssi > -75) barras = 2;
        else                 barras = 1;
    }
    for (int i = 0; i < 4; i++) {
        int h  = 2 + i * 2;             // alturas 2, 4, 6, 8
        int bx = x + i * 3;
        int by = y + 8 - h;
        if (i < barras) oled.fillRect(bx, by, 2, h, SSD1306_WHITE);
        else            oled.drawPixel(bx, y + 7, SSD1306_WHITE);  // base punteada
    }
    if (!conectado) {
        oled.setCursor(x + 13, y);
        oled.print("x");
    }
}

// Etiqueta MQTT: invertida (fondo blanco) si está conectado, con borde si no
void oledEtiquetaMQTT(int x, int y) {
    if (mqttConectado) {
        oled.fillRect(x, y, 27, 9, SSD1306_WHITE);
        oled.setTextColor(SSD1306_BLACK);
        oled.setCursor(x + 2, y + 1);
        oled.print("MQTT");
        oled.setTextColor(SSD1306_WHITE);
    } else {
        oled.drawRect(x, y, 27, 9, SSD1306_WHITE);
        oled.setCursor(x + 2, y + 1);
        oled.print("mqtt");
    }
}

void oledBarraEstado(int pagina, bool parpadeo) {
    oledIconoWiFi(0, 0);
    oledEtiquetaMQTT(24, 0);

    bool hayAlarma = alarmaLluvia || vientoFuerte || alarmaHelada || alarmaTormenta;
    if (hayAlarma && parpadeo) {
        oled.fillRect(62, 0, 27, 9, SSD1306_WHITE);
        oled.setTextColor(SSD1306_BLACK);
        oled.setCursor(65, 1);
        oled.print("ALR!");
        oled.setTextColor(SSD1306_WHITE);
    }

    oled.setCursor(110, 1);
    oled.printf("%d/%d", pagina + 1, OLED_NUM_PAGINAS);

    oled.drawFastHLine(0, 11, OLED_ANCHO, SSD1306_WHITE);
}

// Imprime "°C" usando el carácter 248 del set CP437
void oledGradosC() {
    oled.write(248);
    oled.print("C");
}

// Barra horizontal de aceleración: escala 0–2 g con marca en el umbral
void oledBarraAccel(int x, int y, int ancho, float valor_g) {
    const float escala = 2.0f;
    float v = fabsf(valor_g);
    if (v > escala) v = escala;
    int lleno = (int)(v / escala * ancho);
    oled.drawRect(x, y, ancho, 6, SSD1306_WHITE);
    oled.fillRect(x, y, lleno, 6, SSD1306_WHITE);
    int marca = x + (int)(UMBRAL_VIBRACION / escala * ancho);
    oled.drawFastVLine(marca, y - 2, 10, SSD1306_WHITE);
}

void oledPaginaAire() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("EXTERIOR (DHT11)");

    if (!dhtValido) {
        oled.setTextSize(2);
        oled.setCursor(0, 32);
        oled.print("SIN DATO");
        return;
    }
    oled.setTextSize(2);
    oled.setCursor(0, 28);
    oled.printf("%5.1f", (float)t_dht);
    oledGradosC();
    oled.setCursor(0, 47);
    oled.printf("%5.1f %%", (float)h_dht);
}

void oledPaginaLluviaLuz() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.printf("LLUVIA (ADC %d)", (int)lluvia);

    oled.setTextSize(2);
    oled.setCursor(0, 28);
    oled.print(alarmaLluvia ? "LLUEVE" : "SECO");

    // Luz como índice relativo 0–100 % (LDR sin calibrar, no son lux reales)
    oled.setCursor(0, 47);
    oled.printf("Luz %3d%%", (int)lux / 10);
}

void oledPaginaViento() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    if (!mpuOK) {
        oled.print("VIENTO (MPU6050)");
        oled.setTextSize(2);
        oled.setCursor(0, 32);
        oled.print("SIN MPU");
        return;
    }
    oled.print(vientoFuerte ? "VIENTO: FUERTE" : "VIENTO: calmo");

    // Ejes horizontales con barra (marca vertical = umbral)
    oled.setCursor(0, 27);
    oled.printf("X%+5.2f", (float)accelX);
    oledBarraAccel(48, 28, 78, accelX);

    oled.setCursor(0, 39);
    oled.printf("Y%+5.2f", (float)accelY);
    oledBarraAccel(48, 40, 78, accelY);

    // Eje Z solo informativo (≈ 1 g en reposo)
    oled.setCursor(0, 52);
    oled.printf("Z%+5.2f g", (float)accelZ);
}

void oledPaginaSuelo() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("SUELO (LM35)");

    oled.setTextSize(2);
    oled.setCursor(0, 28);
    oled.printf("%5.1f", (float)t_suelo);
    oledGradosC();

    oled.setTextSize(1);
    oled.setCursor(0, 52);
    oled.print(alarmaHelada ? "RIESGO DE HELADA" : "Sin riesgo helada");
}

void oledPaginaAlarmas() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("ALARMAS");

    int y = 26;
    int n = 0;
    auto linea = [&](const char* txt) {
        if (y > 56) return;            // máximo 4 líneas visibles
        oled.setCursor(0, y);
        oled.print("> ");
        oled.print(txt);
        y += 10;
        n++;
    };

    if (alarmaTormenta) linea("TORMENTA");
    if (alarmaLluvia)   linea("LLUVIA ACTIVA");
    if (vientoFuerte)   linea("VIENTO FUERTE");
    if (alarmaHelada)   linea("RIESGO HELADA");

    if (n == 0) {
        oled.setTextSize(2);
        oled.setCursor(0, 34);
        oled.print("TODO OK");
    }
}

void oledPaginaRed() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("RED");

    oled.setCursor(0, 26);
    if (WiFi.status() == WL_CONNECTED) {
        oled.printf("%s %lddBm", WIFI_SSID, (long)WiFi.RSSI());
        oled.setCursor(0, 36);
        oled.print("IP ");
        oled.print(WiFi.localIP());
    } else {
        oled.print("WiFi: desconectado");
        oled.setCursor(0, 36);
        oled.print("IP --");
    }

    oled.setCursor(0, 46);
    if (mqttConectado) oled.print("MQTT: conectado");
    else               oled.printf("MQTT: error %d", (int)mqttEstado);

    oled.setCursor(0, 56);
    uint32_t s = millis() / 1000;
    oled.printf("Uptime %02lu:%02lu:%02lu",
        (unsigned long)(s / 3600), (unsigned long)((s / 60) % 60), (unsigned long)(s % 60));
}

// ============================================================
//  FUNCIONES AUXILIARES
// ============================================================

// Encola un mensaje MQTT genérico para publicación en taskMQTT
void encolarMQTT(const char* topico, const char* payload, bool retain) {
    MqttMessage msg;
    strncpy(msg.topic,   topico,  MQTT_TOPIC_LEN - 1);
    strncpy(msg.payload, payload, MQTT_PAYLOAD_LEN - 1);
    msg.topic[MQTT_TOPIC_LEN - 1]     = '\0';
    msg.payload[MQTT_PAYLOAD_LEN - 1] = '\0';
    msg.retain = retain;

    if (xQueueSend(mqttQueue, &msg, pdMS_TO_TICKS(100)) != pdTRUE) {
        Serial.println("[WARN] Cola MQTT llena, mensaje descartado");
    }
}

// Encola un evento con formato estándar {"estado":"...","valor":X}
void encolarEvento(const char* topico, const char* estado, float valor) {
    char payload[MQTT_PAYLOAD_LEN];
    snprintf(payload, sizeof(payload),
        "{\"estado\":\"%s\",\"valor\":%.1f}", estado, valor);
    encolarMQTT(topico, payload, false);  // eventos con retain=false
}

// LM35: 10mV/°C. Conversión con escala de 5.0V sobre el ADC de 12 bits (0-4095)
float adcToLM35(int raw) {
    float voltaje = raw * (5.0f / 4095.0f);
    return voltaje * 100.0f;
}

// Fotoresistencia: más luz → ADC más bajo → lux más alto
int adcToLux(int raw) {
    return map(constrain(raw, 0, 4095), 4095, 0, 0, 1000);
}

void connectWiFi() {
    Serial.print("[WIFI] Conectando a ");
    Serial.println(WIFI_SSID);
    WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
    int intentos = 0;
    while (WiFi.status() != WL_CONNECTED && intentos < 20) {
        vTaskDelay(500 / portTICK_PERIOD_MS);
        Serial.print(".");
        intentos++;
    }
    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\n[WIFI] Conectado! IP: " + WiFi.localIP().toString());
    } else {
        Serial.println("\n[WIFI] Fallo de conexión. Reintentando en próximo ciclo.");
    }
}

void connectMQTT() {
    int intentos = 0;
    while (!mqtt.connected() && intentos < 5) {
        Serial.print("[MQTT] Conectando...");
        if (mqtt.connect(MQTT_CLIENT_ID)) {
            Serial.println(" OK");
            epocaMQTT++;   // nueva conexion: las tareas republican el estado de sus eventos
        } else {
            Serial.printf(" Error: %d\n", mqtt.state());
            vTaskDelay(2000 / portTICK_PERIOD_MS);
        }
        mqttEstado = mqtt.state();
        intentos++;
    }
}

// Diagnóstico: imprime por Serial el motivo del último reinicio
void imprimirMotivoReset() {
    const char* motivo;
    switch (esp_reset_reason()) {
        case ESP_RST_POWERON:  motivo = "Encendido normal";                 break;
        case ESP_RST_SW:       motivo = "Reinicio por software (ESP.restart)"; break;
        case ESP_RST_PANIC:    motivo = "PANIC / excepción (ver backtrace)"; break;
        case ESP_RST_INT_WDT:  motivo = "Watchdog de interrupciones";       break;
        case ESP_RST_TASK_WDT: motivo = "Watchdog de tareas";               break;
        case ESP_RST_WDT:      motivo = "Otro watchdog";                    break;
        case ESP_RST_BROWNOUT: motivo = "BROWNOUT (caída de alimentación)"; break;
        default:               motivo = "Otro";                             break;
    }
    Serial.printf("[INIT] Motivo del último reset: %s\n", motivo);
    codigoReset = (int)esp_reset_reason();
    const char* corto;
    switch (esp_reset_reason()) {
        case ESP_RST_POWERON:  corto = "POWERON";  break;
        case ESP_RST_EXT:      corto = "EXT";      break;
        case ESP_RST_SW:       corto = "SW";       break;
        case ESP_RST_PANIC:    corto = "PANIC";    break;
        case ESP_RST_INT_WDT:  corto = "INT_WDT";  break;
        case ESP_RST_TASK_WDT: corto = "TASK_WDT"; break;
        case ESP_RST_WDT:      corto = "WDT";      break;
        case ESP_RST_BROWNOUT: corto = "BROWNOUT"; break;
        case ESP_RST_DEEPSLEEP:corto = "DEEPSLEEP";break;
        default:               corto = "OTRO";     break;
    }
    strncpy(motivoResetTxt, corto, sizeof(motivoResetTxt) - 1);
    motivoResetTxt[sizeof(motivoResetTxt) - 1] = 0;
}
