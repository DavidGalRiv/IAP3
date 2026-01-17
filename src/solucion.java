import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import es.upv.iap.pvalderas.http.HTTPClient;
import dao.DAOFactory;
import dao.LocalizacionGPSDAO;
import dao.TrasladoDAO;
import domain.LocalizacionGPS;
import domain.Traslado;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class solucion {

    // --- CONFIGURACIÓN DE ARQUITECTURA ---
    private static final String EXCHANGE_ENTRADA = "exchange.recepcion";
    private static final String COLA_PROCESAMIENTO = "cola.procesamiento";
    private static final String EXCHANGE_SALIDA = "traslados.localizaciones";
    private static final String COLA_BD = "cola.registro.bd";
    private static final String COLA_VISUALIZADOR = "cola.visualizador";

    // --- CONFIGURACIÓN BD ---
    private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "3306";
    private static final String DB_USER = "root";
    private static final String DB_PASS = ""; 
    private static final String DB_NAME = "stc";

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== SISTEMA TRANSIAP - INICIANDO ===");

        // Lanzamos los componentes en hilos paralelos
        new Thread(new Runnable() { public void run() { ejecutarMiddleware(); }}).start();
        new Thread(new Runnable() { public void run() { ejecutarVisualizador(); }}).start();
        new Thread(new Runnable() { public void run() { ejecutarRegistroBD(); }}).start();

        Thread.sleep(3000); // Esperamos a que RabbitMQ configure colas

        System.out.println("\n--- LANZANDO GENERADORES DE PRUEBA ---\n");
        enviarMensajesPrueba();
    }

    // 1. MIDDLEWARE (Generadores -> API -> Exchange Final)
    private static void ejecutarMiddleware() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_ENTRADA, "fanout");
            channel.queueDeclare(COLA_PROCESAMIENTO, false, false, false, null);
            channel.queueBind(COLA_PROCESAMIENTO, EXCHANGE_ENTRADA, "");
            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");

            System.out.println(" [Middleware] Escuchando...");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String mensaje = new String(body, StandardCharsets.UTF_8);
                    try {
                        DatosNormalizados datos = parsearMensajeManual(mensaje);

                        // Enriquecimiento con API REST
                        String urlTime = "https://pedvalar.webs.upv.es/iap/rest/sntn/timestamp";
                        String urlKey = "https://pedvalar.webs.upv.es/iap/rest/sntn/key/" + datos.matricula;
                        
                        String jsonTime = HTTPClient.get(urlTime, "application/json");
                        String jsonKey = HTTPClient.get(urlKey, "application/json");

                        String timestamp = extraerValorJson(jsonTime, "timestamp");
                        String key = extraerValorJson(jsonKey, "appKey");
                        if (key == null) key = "NO-AUTH";

                        // Construcción JSON Final
                        String mensajeFinal = String.format(
                            "{\"coordenadas\": {\"latitud\": %s, \"longitud\": %s}, \"vehiculo\": \"%s\", \"auth\": \"%s\", \"timestamp\": \"%s\"}", 
                            String.valueOf(datos.latitud).replace(",", "."), 
                            String.valueOf(datos.longitud).replace(",", "."), 
                            datos.matricula, key, timestamp
                        );

                        channel.basicPublish(EXCHANGE_SALIDA, "", null, mensajeFinal.getBytes(StandardCharsets.UTF_8));
                        System.out.println(" [Middleware] >> Procesado: " + mensajeFinal);

                    } catch (Exception e) { System.err.println("Error Middleware: " + e.getMessage()); }
                }
            };
            channel.basicConsume(COLA_PROCESAMIENTO, true, consumer);
        } catch (Exception e) { e.printStackTrace(); }
    }

    // 2. VISUALIZADOR
    private static void ejecutarVisualizador() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");
            channel.queueDeclare(COLA_VISUALIZADOR, false, false, false, null);
            channel.queueBind(COLA_VISUALIZADOR, EXCHANGE_SALIDA, "");

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println(" [Visualizador] PANTALLA: " + new String(body, StandardCharsets.UTF_8));
                }
            };
            channel.basicConsume(COLA_VISUALIZADOR, true, consumer);
        } catch (Exception e) {}
    }

    // 3. REGISTRO BD
    private static void ejecutarRegistroBD() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_SALIDA, "fanout");
            channel.queueDeclare(COLA_BD, false, false, false, null);
            channel.queueBind(COLA_BD, EXCHANGE_SALIDA, "");

            final DAOFactory daoFactory = DAOFactory.getCurrentInstance();
            try { daoFactory.connect(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME); } catch (Exception e) {}

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String msg = new String(body, StandardCharsets.UTF_8);
                    try {
                        LocalizacionGPSDAO locDAO = daoFactory.getLocalizacionGPSDAO();
                        TrasladoDAO trasladoDAO = daoFactory.getTrasladoDAO();
                        if (locDAO != null && trasladoDAO != null) {
                            String vehiculo = extraerValorJson(msg, "vehiculo");
                            double lat = extraerCoordenadaManual(msg, "latitud");
                            double lon = extraerCoordenadaManual(msg, "longitud");
                            
                            Traslado t = trasladoDAO.getTrasladoActivoPorVehiculo(vehiculo);
                            if (t != null) {
                                LocalizacionGPS loc = new LocalizacionGPS();
                                loc.setLatitud(lat); loc.setLongitud(lon); loc.setTraslado(t);
                                locDAO.saveLocalizacionGPS(loc);
                                trasladoDAO.updateUltimaLocalizaionTraslado(t, loc);
                                System.out.println(" [BD] Guardado OK: " + vehiculo);
                            } else {
                                System.out.println(" [BD] Sin traslado activo: " + vehiculo);
                            }
                        }
                    } catch (Exception e) {}
                }
            };
            channel.basicConsume(COLA_BD, true, consumer);
        } catch (Exception e) {}
    }

    // 4. GENERADORES
    private static void enviarMensajesPrueba() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();
            channel.exchangeDeclare(EXCHANGE_ENTRADA, "fanout");

            String csv = "1234-CSV, 39.46, -0.37";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, csv.getBytes());
            Thread.sleep(500);

            String json = "{ \"type\":\"Feature\", \"geometry\": { \"type\": \"Point\", \"coordinates\": [ 39.48, -0.39 ] }, \"properties\": { \"vehicle\": \"9012-GEO\" } }";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, json.getBytes());
            Thread.sleep(500);

            String kml = "<kml><Placemark><Point><coordinates>39.47, -0.38</coordinates></Point><Vehicle id=\"5678-KML\"/></Placemark></kml>";
            channel.basicPublish(EXCHANGE_ENTRADA, "", null, kml.getBytes());
            
            channel.close(); conn.close();
        } catch (Exception e) { e.printStackTrace(); }
    }

    // UTILIDADES MANUALES
    private static class DatosNormalizados { String matricula; double latitud; double longitud; }

    private static DatosNormalizados parsearMensajeManual(String msg) {
        DatosNormalizados d = new DatosNormalizados();
        msg = msg.trim();
        if (msg.startsWith("{")) { 
            d.matricula = extraerValorJson(msg, "vehicle");
            int s = msg.indexOf("["), e = msg.indexOf("]");
            if (s != -1) { String[] n = msg.substring(s+1, e).split(","); d.latitud=Double.parseDouble(n[0].trim()); d.longitud=Double.parseDouble(n[1].trim()); }
        } else if (msg.startsWith("<kml>")) {
            int s = msg.indexOf("<coordinates>")+13, e = msg.indexOf("</coordinates>");
            String[] n = msg.substring(s, e).split(","); d.latitud=Double.parseDouble(n[0].trim()); d.longitud=Double.parseDouble(n[1].trim());
            int sv = msg.indexOf("id=\"")+4, ev = msg.indexOf("\"", sv); d.matricula = msg.substring(sv, ev);
        } else {
            String[] p = msg.split(","); d.matricula=p[0].trim(); d.latitud=Double.parseDouble(p[1].trim()); d.longitud=Double.parseDouble(p[2].trim());
        }
        return d;
    }
    private static String extraerValorJson(String j, String k) { try { int i=j.indexOf("\""+k+"\""); if(i==-1)return null; int s=j.indexOf("\"", i+k.length()+2)+1, e=j.indexOf("\"", s); return j.substring(s, e); } catch(Exception e){return null;} }
    private static double extraerCoordenadaManual(String j, String k) { try { int i=j.indexOf("\""+k+"\""); if(i==-1)return 0; int s=j.indexOf(":", i)+1, c=j.indexOf(",", s), b=j.indexOf("}", s), e=(c==-1||(b!=-1&&b<c))?b:c; return Double.parseDouble(j.substring(s, e).trim()); } catch(Exception e){return 0;} }
}