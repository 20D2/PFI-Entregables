// ============================================================
//  Sitio A — Invernadero
//  Sistema de telemetría sobre infraestructura de red GPON
//  Proyecto Final Ingeniería Electrónica
//
//  Sensores:
//    - DHT11          → temperatura y humedad del aire interior
//    - FC-28          → nivel de humedad del suelo
//    - LDR            → luminosidad interior
//    - MQ-2           → gases y humo
//    - LM35           → temperatura del suelo interior
//
//  Actuadores (LEDs):
//    - LED_TEMP   (26) → temperatura fuera de rango
//    - LED_HUM    (25) → humedad aire baja
//    - LED_RIEGO  (33) → suelo seco, activar riego
//    - LED_GAS    (27) → gas/humo detectado
//    - LED_HELADA (14) → temperatura suelo < umbral helada
//
//  Display OLED (I2C):
//    - VCC → 3.3V   GND → GND   SDA → GPIO 21   SCL → GPIO 22
//    - Dirección I2C 0x3C (algunos módulos usan 0x3D)
//    - Librerías: "Adafruit SSD1306" y "Adafruit GFX Library"
//
//  Tópicos MQTT publicados:
//    sitioA/dht11              → {"temperatura":25.4,"humedad":60.1}
//    sitioA/h_suelo            → {"raw":2800,"porcentaje":32}
//    sitioA/luminosidad        → {"lux":740}
//    sitioA/gas                → {"nivel":1200}
//    sitioA/lm35               → {"temperatura_suelo":6.4}
//    sitioA/eventos/temp       → {"estado":"TEMP_ALTA","valor":36.2}
//                                {"estado":"HELADA","valor":7.5}
//                                {"estado":"OK","valor":0.0}
//    sitioA/eventos/hum_aire   → {"estado":"HUM_BAJA","valor":35.0}
//                                {"estado":"OK","valor":0.0}
//    sitioA/eventos/riego      → {"estado":"RIEGO_ACTIVO","valor":2900.0}
//                                {"estado":"OK","valor":0.0}
//    sitioA/eventos/gas        → {"estado":"ALARMA","valor":2800.0}
//                                {"estado":"OK","valor":0.0}
//    sitioA/eventos/helada     → {"estado":"HELADA","valor":3.1}
//                                {"estado":"OK","valor":0.0}
//    sitioA/sistema/reinicio   → {"motivo": "POWERON", "codigo": 1}
// ============================================================

#include <Arduino.h>
#include <DHT.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>

// ── Configuración de red ─────────────────────────────────────
#define WIFI_SSID        "MQTT-SitioA"
#define WIFI_PASSWORD    "*****"
#define MQTT_BROKER      "192.168.20.198"
#define MQTT_PORT        1883
#define MQTT_CLIENT_ID   "ESP32_SitioA"

// ── Pines GPIO ───────────────────────────────────────────────
#define PIN_DHT          4    // DHT11 data
#define PIN_SUELO        34   // Humedad suelo analógico (ADC1)
#define PIN_LDR          32   // Fotoresistencia (ADC1)
#define PIN_MQ2          35   // MQ-2 gas/humo (ADC1)
#define PIN_LM35         36   // LM35 temperatura suelo (ADC1, input-only SVP)

#define LED_TEMP         26   // temperatura fuera de rango
#define LED_HUM          25   // humedad aire baja
#define LED_RIEGO        33   // suelo seco, activar riego
#define LED_GAS          27   // gas/humo detectado
#define LED_HELADA       14   // temperatura suelo < umbral

// ── Display OLED ─────────────────────────────────────────────
#define PIN_OLED_SDA     21   
#define PIN_OLED_SCL     22
#define OLED_ADDR        0x3C
#define OLED_ANCHO       128
#define OLED_ALTO        64
#define OLED_RESET       -1   // el módulo de 4 pines no tiene pin RESET

#define INTERVALO_OLED   500  // ms — refresco de pantalla
#define OLED_CICLOS_PAG  6    // refrescos por página (6 × 500 ms = 3 s)
#define OLED_NUM_PAGINAS 5

// ── Umbrales de alerta ───────────────────────────────────────
#define UMBRAL_TEMP_MAX   35.0f   // °C — temperatura máxima invernadero
#define UMBRAL_TEMP_MIN   10.0f   // °C — temperatura mínima aire (helada aire)
#define UMBRAL_HUM_MIN    40.0f   // %  — humedad mínima del aire
#define UMBRAL_SUELO_SEC  3480    // ADC — suelo seco, < 15 % (valor alto = seco)
#define UMBRAL_GAS        2100    // ADC — nivel de gas peligroso
#define UMBRAL_HELADA     5.0f    // °C — suelo, riesgo de helada
// #define UMBRAL_LUZ_MIN  200    // umbral de luz en caso de necesitarse

// ── Validez de lectura DHT11 ─────────────────────────────────
#define DHT_LEC_VAL_MS  30000    // ms — lectura válida por máximo 30 segundos

// ── Intervalos de muestreo ───────────────────────────────────
#define INTERVALO_LENTOS  5000    // ms — DHT11, suelo, luz, LM35
#define INTERVALO_GAS     1000    // ms — MQ-2 (muestreo más frecuente)
#define INTERVALO_MQTT    5000    // ms — publicación periódica de telemetría

// ── Cola MQTT ────────────────────────────────────────────────
// Todos los mensajes MQTT se encolan y se publican desde taskMQTT (Core 1)
// Esto evita accesos concurrentes al cliente PubSubClient
#define MQTT_QUEUE_SIZE  20       // máximo mensajes en cola simultáneos
#define MQTT_TOPIC_LEN   64       // longitud máxima del tópico
#define MQTT_PAYLOAD_LEN 128      // longitud máxima del payload

// ── Offset humedad ────────────────────────────────────────────

#define OFFSET_HUMEDAD   0.0f

struct MqttMessage {
    char topic[MQTT_TOPIC_LEN];
    char payload[MQTT_PAYLOAD_LEN];
    bool retain;
};

QueueHandle_t mqttQueue;

// ── Variables compartidas entre tareas ───────────────────────
volatile float  t_dht    = 0.0f;
volatile float  h_dht    = 0.0f;
volatile int    h_suelo  = 0;
volatile int    lux      = 0;
volatile int    gas      = 0;
volatile float  t_suelo  = 0.0f;

// Validez del DHT11: se actualiza solo cuando la lectura es exitosa
volatile bool   dhtValido    = false;
volatile uint32_t dhtUltimoms = 0;    // millis() de la última lectura válida

// Estado del MQ-2 (false durante el precalentamiento)
volatile bool   gasListo     = false;

// Estado de alarmas (escritas por las tareas de sensores, leídas por el display)
volatile bool   alarmaTemp   = false;
volatile bool   alarmaHum    = false;
volatile bool   alarmaRiego  = false;
volatile bool   alarmaGas    = false;
volatile bool   alarmaHelada = false;

// Estado MQTT (escrito solo por taskMQTT, para no tocar PubSubClient
// desde otra tarea). mqttEstado guarda mqtt.state():
//   0 = conectado, -1 = desconectado, -2 = fallo de conexión,
//  -4 = timeout, 5 = no autorizado, etc.
volatile int    mqttEstado    = -1;
volatile bool   mqttConectado = false;
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
WiFiClient       espClient;
PubSubClient     mqtt(espClient);
Adafruit_SSD1306 oled(OLED_ANCHO, OLED_ALTO, &Wire, OLED_RESET);

// ── Prototipos ───────────────────────────────────────────────
// Tareas
void taskSensoresLentos(void* pvParameters);
void taskGas(void* pvParameters);
void taskMQTT(void* pvParameters);
void taskWiFiWatchdog(void* pvParameters);
void taskDisplay(void* pvParameters);

// Display OLED
void oledIconoWiFi(int x, int y);
void oledEtiquetaMQTT(int x, int y);
void oledBarraEstado(int pagina, bool parpadeo);
void oledGradosC();
void oledPaginaAire();
void oledPaginaSuelo();
void oledPaginaLuzGas();
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
int   porcentajeHumSuelo(int raw);

// ============================================================
//  SETUP
// ============================================================
void setup() {
    Serial.begin(115200);
    delay(1000);
    Serial.println("\n=== Sitio A — Invernadero ===");
    imprimirMotivoReset();

    // ── Pines de salida (LEDs) ──
    pinMode(LED_TEMP,   OUTPUT);
    pinMode(LED_HUM,    OUTPUT);
    pinMode(LED_RIEGO,  OUTPUT);
    pinMode(LED_GAS,    OUTPUT);
    pinMode(LED_HELADA, OUTPUT);

    digitalWrite(LED_TEMP,   LOW);
    digitalWrite(LED_HUM,    LOW);
    digitalWrite(LED_RIEGO,  LOW);
    digitalWrite(LED_GAS,    LOW);
    digitalWrite(LED_HELADA, LOW);

    // ── Display OLED ──
    Wire.begin(PIN_OLED_SDA, PIN_OLED_SCL);
    Wire.setClock(400000);              // I2C rápido: refresco ~25 ms
    bool oledOK = oled.begin(SSD1306_SWITCHCAPVCC, OLED_ADDR);
    if (oledOK) {
        oled.cp437(true);               // habilita el símbolo ° (carácter 248)
        oled.clearDisplay();
        oled.setTextColor(SSD1306_WHITE);
        oled.setTextSize(2);
        oled.setCursor(16, 8);
        oled.print("SITIO A");
        oled.setTextSize(1);
        oled.setCursor(28, 32);
        oled.print("Invernadero");
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

    // ── Tareas FreeRTOS ──
    //  Core 0: sensores rápidos
    //  Core 1: sensores lentos + red (WiFi + MQTT corren junto al stack TCP/IP) y display
    xTaskCreatePinnedToCore(taskSensoresLentos, "SensLentos", 4096, NULL, 1, NULL, 1);
    xTaskCreatePinnedToCore(taskGas,            "Gas_MQ2",    4096, NULL, 2, NULL, 0);
    xTaskCreatePinnedToCore(taskMQTT,           "MQTT_Pub",   8192, NULL, 2, NULL, 1);
    xTaskCreatePinnedToCore(taskWiFiWatchdog,   "WiFi_WD",    4096, NULL, 1, NULL, 1);
    if (oledOK) {
        xTaskCreatePinnedToCore(taskDisplay,    "Display",    4096, NULL, 1, NULL, 1);
    }

    Serial.println("[INIT] Tareas iniciadas. FreeRTOS corriendo.");
}

// ── LOOP vacío — FreeRTOS toma el control ───────────────────
void loop() {
    vTaskDelay(portMAX_DELAY);
}

// ============================================================
//  TAREA 1: Sensores lentos — Core 0
//  DHT11, humedad suelo, fotoresistencia, LM35
//  Frecuencia: cada 5 segundos
// ============================================================
void taskSensoresLentos(void* pvParameters) {
    Serial.println("[TASK] SensoresLentos iniciada");

    // Flags de estado para publicar solo en transición
    bool tempAlertaAct  = false;
    bool humAlertaAct   = false;
    bool sueloSecoAct   = false;
    bool heladaAct      = false;
    uint32_t epocaLocal = 0;       // ultima epoca MQTT sincronizada
    bool resyncDHT      = false;   // republicar eventos del DHT11 en cuanto haya dato valido

    for (;;) {
        // Si hubo una conexion MQTT nueva (o recien arranco), republicar el estado actual
        bool resync = (epocaMQTT != epocaLocal);
        if (resync) { epocaLocal = epocaMQTT; resyncDHT = true; }

        // ── DHT11 ───────────────────────────────────────────
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

        // ── Humedad de suelo (promedio 5 lecturas) ──────────
        int sumaSuelo = 0;
        for (int i = 0; i < 5; i++) {
            sumaSuelo += analogRead(PIN_SUELO);
            vTaskDelay(20 / portTICK_PERIOD_MS);
        }
        h_suelo = sumaSuelo / 5;

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

        // Temperatura del aire (solo si DHT11 tiene dato válido)
        if (dhtValido) {
            bool tempAlerta = (t_dht > UMBRAL_TEMP_MAX || t_dht < UMBRAL_TEMP_MIN);
            alarmaTemp = tempAlerta;
            digitalWrite(LED_TEMP, tempAlerta ? HIGH : LOW);
            if (tempAlerta && (!tempAlertaAct || resyncDHT)) {
                tempAlertaAct = true;
                encolarEvento("sitioA/eventos/temp",
                    t_dht > UMBRAL_TEMP_MAX ? "TEMP_ALTA" : "HELADA", t_dht);
            } else if (!tempAlerta && (tempAlertaAct || resyncDHT)) {
                tempAlertaAct = false;
                encolarEvento("sitioA/eventos/temp", "OK", 0.0f);
            }

            // Humedad del aire baja
            bool humAlerta = (h_dht < UMBRAL_HUM_MIN);
            alarmaHum = humAlerta;
            digitalWrite(LED_HUM, humAlerta ? HIGH : LOW);
            if (humAlerta && (!humAlertaAct || resyncDHT)) {
                humAlertaAct = true;
                encolarEvento("sitioA/eventos/hum_aire", "HUM_BAJA", h_dht);
            } else if (!humAlerta && (humAlertaAct || resyncDHT)) {
                humAlertaAct = false;
                encolarEvento("sitioA/eventos/hum_aire", "OK", 0.0f);
            }
            resyncDHT = false;   // estado del DHT11 ya sincronizado
        } else {
            // DHT11 sin dato válido: apagar LEDs relacionados
            digitalWrite(LED_TEMP, LOW);
            digitalWrite(LED_HUM, LOW);
            alarmaTemp = false;
            alarmaHum  = false;
            // Resetear flags para que al recuperarse publiquen estado actual
            tempAlertaAct = false;
            humAlertaAct  = false;
        }

        // Suelo seco → riego
        bool sueloSeco = (h_suelo > UMBRAL_SUELO_SEC);
        alarmaRiego = sueloSeco;
        digitalWrite(LED_RIEGO, sueloSeco ? HIGH : LOW);
        if (sueloSeco && (!sueloSecoAct || resync)) {
            sueloSecoAct = true;
            encolarEvento("sitioA/eventos/riego", "RIEGO_ACTIVO", h_suelo);
        } else if (!sueloSeco && (sueloSecoAct || resync)) {
            sueloSecoAct = false;
            encolarEvento("sitioA/eventos/riego", "OK", 0.0f);
        }

        // Helada por temperatura del suelo
        bool heladaRiesgo = (t_suelo < UMBRAL_HELADA);
        alarmaHelada = heladaRiesgo;
        digitalWrite(LED_HELADA, heladaRiesgo ? HIGH : LOW);
        if (heladaRiesgo && (!heladaAct || resync)) {
            heladaAct = true;
            encolarEvento("sitioA/eventos/helada", "HELADA", t_suelo);
        } else if (!heladaRiesgo && (heladaAct || resync)) {
            heladaAct = false;
            encolarEvento("sitioA/eventos/helada", "OK", 0.0f);
        }

        Serial.printf("[SENSL] T:%.1f°C H:%.1f%% Hsuelo:%d(%d%%) Luz:%d Tsuelo:%.1f°C DHT:%s\n",
            t_dht, h_dht, h_suelo, porcentajeHumSuelo(h_suelo), lux, t_suelo,
            dhtValido ? "OK" : "INVALIDO");

        vTaskDelay(INTERVALO_LENTOS / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 2: MQ-2 Gas/Humo — Core 0
//  Precalentamiento de 20s al arranque, muestreo cada 1s
// ============================================================
void taskGas(void* pvParameters) {
    Serial.println("[TASK] Gas MQ-2 iniciada — precalentando 20s...");
    vTaskDelay(20000 / portTICK_PERIOD_MS);
    gasListo = true;
    Serial.println("[TASK] MQ-2 listo");

    bool gasAlertaAct = false;
    uint32_t epocaGas = 0;

    for (;;) {
        int suma = 0;
        for (int i = 0; i < 5; i++) {
            suma += analogRead(PIN_MQ2);
            vTaskDelay(50 / portTICK_PERIOD_MS);
        }
        gas = suma / 5;
        bool resync = (epocaMQTT != epocaGas);
        epocaGas = epocaMQTT;

        bool gasAlerta = (gas > UMBRAL_GAS);
        alarmaGas = gasAlerta;
        digitalWrite(LED_GAS, gasAlerta ? HIGH : LOW);

        if (gasAlerta && (!gasAlertaAct || resync)) {
            gasAlertaAct = true;
            encolarEvento("sitioA/eventos/gas", "ALARMA", gas);
            Serial.printf("[ALARMA] Gas detectado! ADC: %d\n", gas);
        } else if (!gasAlerta && (gasAlertaAct || resync)) {
            gasAlertaAct = false;
            encolarEvento("sitioA/eventos/gas", "OK", 0.0f);
        }

        Serial.printf("[SENSL] Gas:%d\n", gas);
        vTaskDelay(INTERVALO_GAS / portTICK_PERIOD_MS);
    }
}

// ============================================================
//  TAREA 3: Publicación MQTT — Core 1
//  Publica telemetría periódica y drena la cola de eventos
// ============================================================
void taskMQTT(void* pvParameters) {
    Serial.println("[TASK] MQTT iniciada");

    for (;;) {
        // Reconecta si es necesario
        if (!mqtt.connected()) {
            mqttConectado = false;
            connectMQTT();
        }
        mqtt.loop();

        // Publica el estado para el display (solo esta tarea toca PubSubClient)
        mqttConectado = mqtt.connected();
        mqttEstado    = mqtt.state();

        if (mqtt.connected()) {
            // ── Motivo del último reinicio (una sola vez por arranque) ──
            if (!resetPublicado) {
                char rbuf[96];
                snprintf(rbuf, sizeof(rbuf), "{\"motivo\":\"%s\",\"codigo\":%d}", motivoResetTxt, codigoReset);
                if (mqtt.publish("sitioA/sistema/reinicio", rbuf)) {
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
                mqtt.publish("sitioA/dht11", buf);
            }

            snprintf(buf, sizeof(buf), "{\"raw\":%d,\"porcentaje\":%d}",
                h_suelo, porcentajeHumSuelo(h_suelo));
            mqtt.publish("sitioA/h_suelo", buf);

            snprintf(buf, sizeof(buf), "{\"lux\":%d}", lux);
            mqtt.publish("sitioA/luminosidad", buf);

            snprintf(buf, sizeof(buf), "{\"nivel\":%d}", gas);
            mqtt.publish("sitioA/gas", buf);

            snprintf(buf, sizeof(buf), "{\"temperatura_suelo\":%.1f}", (float)t_suelo);
            mqtt.publish("sitioA/lm35", buf);

            Serial.println("[MQTT] Telemetría publicada");

            // ── Drena la cola de eventos ─────────────────────
            // Se procesan todos los mensajes pendientes antes del próximo ciclo
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
//  Es la única tarea que accede al bus I2C / objeto oled
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
            case 0: oledPaginaAire();    break;
            case 1: oledPaginaSuelo();   break;
            case 2: oledPaginaLuzGas();  break;
            case 3: oledPaginaAlarmas(); break;
            case 4: oledPaginaRed();     break;
        }

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

    bool hayAlarma = alarmaTemp || alarmaHum || alarmaRiego || alarmaGas || alarmaHelada;
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

void oledPaginaAire() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("AIRE (DHT11)");

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

void oledPaginaSuelo() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.printf("SUELO (ADC %d)", (int)h_suelo);

    oled.setTextSize(2);
    oled.setCursor(0, 28);
    oled.printf("H %3d %%", porcentajeHumSuelo(h_suelo));
    oled.setCursor(0, 47);
    oled.printf("T%5.1f", (float)t_suelo);
    oledGradosC();
}

void oledPaginaLuzGas() {
    oled.setTextSize(1);
    oled.setCursor(0, 15);
    oled.print("LUZ / GAS (MQ-2)");

    oled.setTextSize(2);
    oled.setCursor(0, 28);

    // Luz como índice relativo 0–100 % (LDR sin calibrar, no son lux reales)
    oled.printf("L %3d%%", (int)lux /10);
    oled.setCursor(0, 47);
    if (gasListo) {
        oled.printf("G %4d", (int)gas);
    } else {
        oled.setTextSize(1);
        oled.setCursor(0, 50);
        oled.print("Gas: precalentando...");
    }
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

    if (alarmaTemp)   linea(t_dht > UMBRAL_TEMP_MAX ? "TEMP AIRE ALTA" : "TEMP AIRE BAJA");
    if (alarmaHum)    linea("HUMEDAD AIRE BAJA");
    if (alarmaRiego)  linea("SUELO SECO - RIEGO");
    if (alarmaGas)    linea("GAS / HUMO");
    if (alarmaHelada) linea("RIESGO HELADA");

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

// Humedad de suelo: más seco → ADC más alto → porcentaje más bajo
int porcentajeHumSuelo(int raw) {
    return map(constrain(raw, 0, 4095), 4095, 0, 0, 100);
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
        case ESP_RST_POWERON:  motivo = "Encendido normal";                    break;
        case ESP_RST_SW:       motivo = "Reinicio por software (ESP.restart)"; break;
        case ESP_RST_PANIC:    motivo = "PANIC / excepción (ver backtrace)";   break;
        case ESP_RST_INT_WDT:  motivo = "Watchdog de interrupciones";          break;
        case ESP_RST_TASK_WDT: motivo = "Watchdog de tareas";                  break;
        case ESP_RST_WDT:      motivo = "Otro watchdog";                       break;
        case ESP_RST_BROWNOUT: motivo = "BROWNOUT (caída de alimentación)";    break;
        default:               motivo = "Otro";                                break;
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
