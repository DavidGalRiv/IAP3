package tests;
import es.upv.iap.pvalderas.http.HTTPClient;

public class TestHTTP {

    public static void main(String[] args) {
        System.out.println("=== INICIANDO TEST DE CONEXIÓN HTTP ===");

        // 1. URL del servicio Timestamp (Fuente: PDF Sección 5)
        String urlTimestamp = "https://pedvalar.webs.upv.es/iap/rest/sntn/timestamp";
        
        // 2. URL del servicio Key (Fuente: PDF Sección 5) - Usamos una matrícula de prueba
        String matriculaPrueba = "1234-TEST";
        String urlKey = "https://pedvalar.webs.upv.es/iap/rest/sntn/key/" + matriculaPrueba;

        try {
            // PRUEBA 1: Obtener Timestamp
            System.out.println("\n[1] Solicitando Timestamp...");
            // El segundo parámetro es el mimeType, según el PDF debe ser "application/json"
            String respuestaTime = HTTPClient.get(urlTimestamp, "application/json");
            System.out.println("    >> Respuesta Recibida: " + respuestaTime);

            // PRUEBA 2: Obtener Key
            System.out.println("\n[2] Solicitando Key para matrícula " + matriculaPrueba + "...");
            String respuestaKey = HTTPClient.get(urlKey, "application/json");
            System.out.println("    >> Respuesta Recibida: " + respuestaKey);

            if (respuestaTime != null && respuestaKey != null) {
                System.out.println("\n✅ EL TEST HA SIDO EXITOSO. LA LIBRERÍA FUNCIONA.");
            } else {
                System.out.println("\n⚠️ SE RECIBIERON RESPUESTAS VACÍAS.");
            }

        } catch (Exception e) {
            System.err.println("\n❌ ERROR DE CONEXIÓN:");
            System.err.println("   Asegúrate de haber añadido 'cliente_http.jar' a las librerías referenciadas.");
            System.err.println("   Detalle del error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}