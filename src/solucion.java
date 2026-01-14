import com.rabbitmq.client.*;

// Importaciones de los JARs proporcionados por los profesores
import es.upv.iap.pvalderas.http.HTTPClient;
import dao.DAOFactory;
import dao.LocalizacionGPSDAO;
import dao.TrasladoDAO;
import domain.LocalizacionGPS;
import domain.Traslado;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class solucion { // <--- CLASE RENOMBRADA PARA COINCIDIR CON TU ARCHIVO

    // --- CONFIGURACIÓN GLOBAL ---
    private static final String HOST = "localhost";
    private static final String EXCHANGE_ENTRADA = "exchange.entrada";
    private static final String EXCHANGE_FINAL = "traslados.localizaciones";
    
    // Configuración BD (Ajusta el pass si es necesario)
    private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "3306";
    private static final String DB_USER = "root";
    private static final String DB_PASS = ""; 
    private static final String DB_NAME = "stc";

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== SISTEMA TRANSIAP (SOLUCION FINAL) ===");

        // Cambiado "Main::" por "solucion::"
        new Thread(solucion::ejecutarMiddleware, "Hilo-Middleware").start();
        new Thread(solucion::ejecutarVisualizador, "Hilo-Visualizador").start();
        new Thread(solucion::ejecutarRegistroBD, "Hilo-RegistroBD").start();

        Thread.sleep(2000);

        System.out.println("\n--- LANZANDO GENERADORES DE PRUEBA ---\n");
        enviarMensajesPrueba();
    }

    // =========================================================================
    // 1. LÓGICA DEL MIDDLEWARE (Parseo manual de Strings)
    // =========================================================================
    private static void ejecutarMiddleware() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_ENTRADA, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_ENTRADA, "");

            channel.exchangeDeclare(EXCHANGE_FINAL, BuiltinExchangeType.FANOUT);

            System.out.println(" [Middleware] Escuchando...");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String mensaje = new String(delivery.getBody(), StandardCharsets.UTF_8);
                try {
                    DatosNormalizados datos = parsearMensajeManual(mensaje);

                    String urlTimestamp = "https://pedvalar.webs.upv.es/iap/rest/sntn/timestamp";
                    String urlKey = "https://pedvalar.webs.upv.es/iap/rest/sntn/key/" + datos.matricula;

                    String jsonTime = HTTPClient.get(urlTimestamp, "application/json");
                    String jsonKey = HTTPClient.get(urlKey, "application/json");

                    String timestamp = extraerValorJson(jsonTime, "timestamp");
                    String key = extraerValorJson(jsonKey, "appKey");
                    if (key == null || key.isEmpty()) key = "ERROR-KEY";

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

                    channel.basicPublish(EXCHANGE_FINAL, "", null, mensajeFinal.getBytes(StandardCharsets.UTF_8));
                    System.out.println(" [Middleware] -> Procesado: " + mensajeFinal);

                } catch (Exception e) {
                    System.err.println(" [Middleware] Error: " + e.getMessage());
                }
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        } catch (Exception e) { e.printStackTrace(); }
    }

    // =========================================================================
    // 2. VISUALIZADOR
    // =========================================================================
    private static void ejecutarVisualizador() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_FINAL, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_FINAL, "");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String mensaje = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [Visualizador] PANTALLA >> " + mensaje);
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        } catch (Exception e) {}
    }

    // =========================================================================
    // 3. REGISTRO BD
    // =========================================================================
    private static void ejecutarRegistroBD() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_FINAL, BuiltinExchangeType.FANOUT);
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_FINAL, "");

            DAOFactory daoFactory = DAOFactory.getCurrentInstance();
            try {
                daoFactory.connect(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME);
            } catch (Exception e) {
                System.err.println(" [BD] Aviso: No se pudo conectar a MySQL.");
                return;
            }

            LocalizacionGPSDAO locDAO = daoFactory.getLocalizacionGPSDAO();
            TrasladoDAO trasladoDAO = daoFactory.getTrasladoDAO();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String mensaje = new String(delivery.getBody(), StandardCharsets.UTF_8);
                try {
                    String vehiculo = extraerValorJson(mensaje, "vehiculo");
                    double lat = extraerCoordenadaManual(mensaje, "latitud");
                    double lon = extraerCoordenadaManual(mensaje, "longitud");

                    Traslado traslado = trasladoDAO.getTrasladoActivoPorVehiculo(vehiculo);
                    if (traslado != null) {
                        LocalizacionGPS loc = new LocalizacionGPS();
                        loc.setLatitud(lat);
                        loc.setLongitud(lon);
                        loc.setTraslado(traslado);
                        locDAO.saveLocalizacionGPS(loc);
                        trasladoDAO.updateUltimaLocalizaionTraslado(traslado, loc);
                        System.out.println(" [BD] Guardado OK: " + vehiculo);
                    }
                } catch (Exception e) {}
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        } catch (Exception e) {}
    }

    // =========================================================================
    // 4. GENERADORES
    // =========================================================================
    private static void enviarMensajesPrueba() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST);
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.exchangeDeclare(EXCHANGE_ENTRADA, BuiltinExchangeType.FANOUT);

                // CSV
                String msgCSV = "1234-CSV, 39.4699, -0.3762";
                channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgCSV.getBytes());
                Thread.sleep(500);

                // GeoJSON
                String msgJSON = "{ \"type\":\"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 39.4800, -0.3900 ] }, \"properties\": { \"vehicle\": \"9012-GEO\" } }";
                channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgJSON.getBytes());
                Thread.sleep(500);

                // KML
                String msgKML = "<kml><Placemark><Point><coordinates>39.4750, -0.3800</coordinates></Point><Vehicle id=\"5678-KML\"/></Placemark></kml>";
                channel.basicPublish(EXCHANGE_ENTRADA, "", null, msgKML.getBytes());
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    // =========================================================================
    // UTILIDADES: PARSEO MANUAL
    // =========================================================================
    
    private static class DatosNormalizados {
        String matricula;
        double latitud;
        double longitud;
    }

    private static DatosNormalizados parsearMensajeManual(String msg) {
        DatosNormalizados d = new DatosNormalizados();
        msg = msg.trim();

        if (msg.startsWith("{")) { 
            d.matricula = extraerValorJson(msg, "vehicle");
            int startBracket = msg.indexOf("[");
            int endBracket = msg.indexOf("]");
            if (startBracket != -1 && endBracket != -1) {
                String content = msg.substring(startBracket + 1, endBracket);
                String[] nums = content.split(",");
                d.latitud = Double.parseDouble(nums[0].trim());
                d.longitud = Double.parseDouble(nums[1].trim());
            }
        } else if (msg.startsWith("<kml>")) { 
            int startCoord = msg.indexOf("<coordinates>") + 13;
            int endCoord = msg.indexOf("</coordinates>");
            String[] coords = msg.substring(startCoord, endCoord).trim().split(",");
            d.latitud = Double.parseDouble(coords[0].trim());
            d.longitud = Double.parseDouble(coords[1].trim());
            
            int startVeh = msg.indexOf("Vehicle id=\"") + 12;
            int endVeh = msg.indexOf("\"", startVeh);
            d.matricula = msg.substring(startVeh, endVeh);
        } else { 
            String[] partes = msg.split(",");
            d.matricula = partes[0].trim();
            d.latitud = Double.parseDouble(partes[1].trim());
            d.longitud = Double.parseDouble(partes[2].trim());
        }
        return d;
    }

    private static String extraerValorJson(String json, String key) {
        try {
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
            
            String numStr = json.substring(colon + 1, end).trim();
            return Double.parseDouble(numStr);
        } catch (Exception e) { return 0.0; }
    }
}