import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

// Importaciones de los JARs proporcionados por los profesores
import es.upv.iap.pvalderas.http.HTTPClient;
import dao.DAOFactory;
import dao.LocalizacionGPSDAO;
import dao.TrasladoDAO;
import domain.LocalizacionGPS;
import domain.Traslado;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class solucion {

    // --- DISEÑO DE ARQUITECTURA RABBITMQ ---
    // 1. Entrada: Donde los camiones (Generadores) dejan sus datos crudos
    private static final String EXCHANGE_ENTRADA = "exchange.recepcion";
    private static final String COLA_PROCESAMIENTO = "cola.procesamiento";
    
    // 2. Salida: Donde el Middleware deja el JSON final (OBLIGATORIO SEGÚN PDF)
    private static final String EXCHANGE_SALIDA = "traslados.localizaciones";
    
    // 3. Colas de Consumo Final
    private static final String COLA_BD = "cola.registro.bd";
    private static final String COLA_VISUALIZADOR = "cola.visualizador";

    // Configuración BD
    private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "3306";
    private static final String DB_USER = "root";
    private static final String DB_PASS = ""; 
    private static final String DB_NAME = "stc";

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== INICIANDO SISTEMA TRANSIAP (VERSION COMPATIBLE JAR ANTIGUO) ===");

        // Iniciar los componentes en hilos separados
        new Thread(new Runnable() { public void run() { ejecutarMiddleware(); }}).start();
        new Thread(new Runnable() { public void run() { ejecutarVisualizador(); }}).start();
        new Thread(new Runnable() { public void run() { ejecutarRegistroBD(); }}).start();

        // Esperar a que la infraestructura RabbitMQ se levante
        Thread.sleep(3000);

        System.out.println("\n--- SIMULANDO CAMIONES (GENERADORES) ---\n");
        enviarMensajesPrueba();
    }

    // =========================================================================
    // 1. MIDDLEWARE: SOPORTE LOGÍSTICA 'LIVE'
    // Recibe de Generadores -> Normaliza -> Enriquece (API) -> Publica en traslados.localizaciones
    // =========================================================================
    private static void ejecutarMiddleware() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // 1. Configurar recepción (Entrada)
            channel.exchangeDeclare(EXCHANGE_ENTRADA, "fanout"); // BuiltinExchangeType puede no existir en versiones muy viejas
            channel.queueDeclare(COLA_PROCESAMIENTO, false, false, false, null);
            channel.queueBind(COLA_PROCESAMIENTO, EXCHANGE_ENTRADA, "");

            // 2. Configurar emisión (Salida - Requisito PDF)
            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");

            System.out.println(" [Middleware] Listo. Escuchando datos de camiones...");

            // USAMOS DefaultConsumer PARA COMPATIBILIDAD CON JAR ANTIGUO
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    
                    String mensaje = new String(body, StandardCharsets.UTF_8);
                    try {
                        // A. Normalizar (KML/CSV/GeoJSON -> Objeto Java)
                        DatosNormalizados datos = parsearMensajeManual(mensaje);

                        // B. Enriquecer (Llamada a API REST SNTN)
                        String urlTimestamp = "https://pedvalar.webs.upv.es/iap/rest/sntn/timestamp";
                        String urlKey = "https://pedvalar.webs.upv.es/iap/rest/sntn/key/" + datos.matricula;

                        String jsonTime = HTTPClient.get(urlTimestamp, "application/json");
                        String jsonKey = HTTPClient.get(urlKey, "application/json");

                        // Extraer datos simples de la respuesta API
                        String timestamp = extraerValorJson(jsonTime, "timestamp");
                        String key = extraerValorJson(jsonKey, "appKey");
                        if (key == null) key = "NO-AUTH";

                        // C. Crear JSON Final (TransIAP-loc/JSON)
                        String mensajeFinal = String.format(
                            "{" +
                                "\"coordenadas\": {" +
                                    "\"latitud\": %s," +
                                    "\"longitud\": %s" +
                                "}," +
                                "\"vehiculo\": \"%s\"," +
                                "\"auth\": \"%s\"," +
                                "\"timestamp\": \"%s\"" +
                            "}", 
                            String.valueOf(datos.latitud).replace(",", "."), 
                            String.valueOf(datos.longitud).replace(",", "."), 
                            datos.matricula, 
                            key, 
                            timestamp
                        );

                        // D. Publicar al Exchange oficial
                        channel.basicPublish(EXCHANGE_SALIDA, "", null, mensajeFinal.getBytes(StandardCharsets.UTF_8));
                        System.out.println(" [Middleware] PROCESADO >> " + mensajeFinal);

                    } catch (Exception e) {
                        System.err.println(" [Middleware] Error procesando: " + e.getMessage());
                    }
                }
            };

            channel.basicConsume(COLA_PROCESAMIENTO, true, consumer);

        } catch (Exception e) { e.printStackTrace(); }
    }

    // =========================================================================
    // 2. VISUALIZADOR DE LOCALIZACIONES
    // Escucha de traslados.localizaciones y muestra en pantalla
    // =========================================================================
    private static void ejecutarVisualizador() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Conectar al Exchange Salida
            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");
            // Cola exclusiva para el visualizador
            channel.queueDeclare(COLA_VISUALIZADOR, false, false, false, null);
            channel.queueBind(COLA_VISUALIZADOR, EXCHANGE_SALIDA, "");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String mensaje = new String(body, StandardCharsets.UTF_8);
                    System.out.println(" [Visualizador] PANTALLA: " + mensaje);
                }
            };
            channel.basicConsume(COLA_VISUALIZADOR, true, consumer);
        } catch (Exception e) {}
    }

    // =========================================================================
    // 3. REGISTRO EN BD (STC DAO)
    // Escucha de traslados.localizaciones y guarda en MySQL
    // =========================================================================
    private static void ejecutarRegistroBD() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Conectar al Exchange Salida
            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");
            channel.queueDeclare(COLA_BD, false, false, false, null);
            channel.queueBind(COLA_BD, EXCHANGE_SALIDA, "");

            // Conexión a Base de Datos
            final DAOFactory daoFactory = DAOFactory.getCurrentInstance();
            try {
                daoFactory.connect(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME);
            } catch (Exception e) {
                System.err.println(" [BD] ERROR: No se puede conectar a MySQL. Verifica XAMPP.");
            }

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String mensaje = new String(body, StandardCharsets.UTF_8);
                    try {
                        LocalizacionGPSDAO locDAO = daoFactory.getLocalizacionGPSDAO();
                        TrasladoDAO trasladoDAO = daoFactory.getTrasladoDAO();
                        if (locDAO == null || trasladoDAO == null) return;

                        String vehiculo = extraerValorJson(mensaje, "vehiculo");
                        double lat = extraerCoordenadaManual(mensaje, "latitud");
                        double lon = extraerCoordenadaManual(mensaje, "longitud");

                        // Lógica de Negocio dada en PDF
                        Traslado traslado = trasladoDAO.getTrasladoActivoPorVehiculo(vehiculo);
                        if (traslado != null) {
                            LocalizacionGPS loc = new LocalizacionGPS();
                            loc.setLatitud(lat);
                            loc.setLongitud(lon);
                            loc.setTraslado(traslado);
                            
                            locDAO.saveLocalizacionGPS(loc);
                            trasladoDAO.updateUltimaLocalizaionTraslado(traslado, loc);
                            System.out.println(" [BD] Guardado correctamente: " + vehiculo);
                        } else {
                            System.out.println(" [BD] Vehículo sin traslado activo: " + vehiculo);
                        }
                    } catch (Exception e) {
                        // Ignorar errores puntuales de parseo o SQL
                    }
                }
            };
            channel.basicConsume(COLA_BD, true, consumer);

        } catch (Exception e) {}
    }

    // =========================================================================
    // 4. GENERADORES (Simulación de camiones enviando datos)
    // =========================================================================
    private static void enviarMensajesPrueba() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_ENTRADA, "fanout");

            // Generador 1: CSV
            String msgCSV = "1234-CSV, 39.4699, -0.3762";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgCSV.getBytes());
            Thread.sleep(500);

            // Generador 2: GeoJSON
            String msgJSON = "{ \"type\":\"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 39.4800, -0.3900 ] }, \"properties\": { \"vehicle\": \"9012-GEO\" } }";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgJSON.getBytes());
            Thread.sleep(500);

            // Generador 3: KML
            String msgKML = "<kml><Placemark><Point><coordinates>39.4750, -0.3800</coordinates></Point><Vehicle id=\"5678-KML\"/></Placemark></kml>";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgKML.getBytes());
            
            channel.close();
            connection.close();

        } catch (Exception e) { e.printStackTrace(); }
    }

    // =========================================================================
    // UTILIDADES (Parseo manual sin librerías externas)
    // =========================================================================
    private static class DatosNormalizados {
        String matricula;
        double latitud;
        double longitud;
    }

    private static DatosNormalizados parsearMensajeManual(String msg) {
        DatosNormalizados d = new DatosNormalizados();
        msg = msg.trim();
        if (msg.startsWith("{")) { // GeoJSON
            d.matricula = extraerValorJson(msg, "vehicle");
            int startBracket = msg.indexOf("[");
            int endBracket = msg.indexOf("]");
            if (startBracket != -1 && endBracket != -1) {
                String content = msg.substring(startBracket + 1, endBracket);
                String[] nums = content.split(",");
                d.latitud = Double.parseDouble(nums[0].trim());
                d.longitud = Double.parseDouble(nums[1].trim());
            }
        } else if (msg.startsWith("<kml>")) { // KML
            int startCoord = msg.indexOf("<coordinates>") + 13;
            int endCoord = msg.indexOf("</coordinates>");
            String[] coords = msg.substring(startCoord, endCoord).trim().split(",");
            d.latitud = Double.parseDouble(coords[0].trim());
            d.longitud = Double.parseDouble(coords[1].trim());
            int startVeh = msg.indexOf("Vehicle id=\"") + 12;
            int endVeh = msg.indexOf("\"", startVeh);
            d.matricula = msg.substring(startVeh, endVeh);
        } else { // CSV
            String[] partes = msg.split(",");
            d.matricula = partes[0].trim();
            d.latitud = Double.parseDouble(partes[1].trim());
            d.longitud = Double.parseDouble(partes[2].trim());
        }
        return d;
    }

    private static String extraerValorJson(String json, String key) {
        try {
            if (json == null) return null;
            String buscar = "\"" + key + "\"";
            int idx = json.indexOf(buscar);
            if (idx == -1) return null;
            int startQuote = json.indexOf("\"", idx + buscar.length() + 1);
            if (startQuote == -1) return null;
            int endQuote = json.indexOf("\"", startQuote + 1);
            return json.substring(startQuote + 1, endQuote);
        } catch (Exception e) { return null; }
    }

    private static double extraerCoordenadaManual(String json, String key) {
        try {
            String buscar = "\"" + key + "\"";
            int idx = json.indexOf(buscar);
            if (idx == -1) return 0.0;
            int colon = json.indexOf(":", idx);
            int comma = json.indexOf(",", colon);
            int brace = json.indexOf("}", colon);
            int end = comma;
            if (end == -1 || (brace != -1 && brace < end)) end = brace;
            return Double.parseDouble(json.substring(colon + 1, end).trim());
        } catch (Exception e) { return 0.0; }
    }
}