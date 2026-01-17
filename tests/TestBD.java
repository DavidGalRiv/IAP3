package tests;
import dao.DAOFactory;
import dao.TrasladoDAO;
import domain.Traslado;

public class TestBD {
    public static void main(String[] args) {
        System.out.println("=== TEST DE CONEXIÓN A BASE DE DATOS ===");

        // Configuración (Asegúrate de que coincide con tu XAMPP/MySQL)
        String dbHost = "localhost";
        String dbPort = "3306";
        String dbUser = "root";
        String dbPass = "";     // Pon contraseña si tienes
        String dbName = "stc";

        try {
            // 1. Intentar conectar
            DAOFactory daoFactory = DAOFactory.getCurrentInstance();
            daoFactory.connect(dbHost, dbPort, dbUser, dbPass, dbName);
            System.out.println("✅ Conexión establecida con MySQL.");

            // 2. Intentar una consulta real
            TrasladoDAO trasladoDAO = daoFactory.getTrasladoDAO();
            
            // Usamos una matrícula de ejemplo. Si la BD está vacía, devolverá null (pero no error).
            String matriculaTest = "1234-CSV"; 
            System.out.println("🔎 Buscando traslado activo para: " + matriculaTest);
            
            Traslado t = trasladoDAO.getTrasladoActivoPorVehiculo(matriculaTest);

            if (t != null) {
                System.out.println("✅ Se encontró un traslado (ID: " + t.getId() + ")");
            } else {
                System.out.println("⚠️ La conexión funciona, pero no hay traslado activo para " + matriculaTest + " (Esto es normal si la BD está vacía).");
            }

        } catch (Exception e) {
            System.err.println("❌ ERROR CRÍTICO DE BD:");
            System.err.println("   Revisa que XAMPP esté encendido y el usuario/pass sean correctos.");
            e.printStackTrace();
        }
    }
}