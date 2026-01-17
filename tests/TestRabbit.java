package tests;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class TestRabbit {
    public static void main(String[] args) {
        System.out.println("=== TEST DE CONEXIÓN RABBITMQ ===");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        // factory.setUsername("guest"); // Opcional, por defecto es guest
        // factory.setPassword("guest"); // Opcional, por defecto es guest

        try {
            // 1. Intentar conectar al servidor
            Connection connection = factory.newConnection();
            System.out.println("✅ Conexión establecida con RabbitMQ Server.");

            // 2. Intentar abrir un canal
            Channel channel = connection.createChannel();
            System.out.println("✅ Canal creado correctamente.");

            // 3. Declarar una cola temporal de prueba
            String queueName = channel.queueDeclare().getQueue();
            System.out.println("✅ Cola temporal creada: " + queueName);

            channel.close();
            connection.close();
            System.out.println("👋 Test finalizado con éxito.");

        } catch (java.net.ConnectException e) {
            System.err.println("❌ ERROR: CONNECTION REFUSED");
            System.err.println("   El servidor RabbitMQ está APAGADO o no instalado.");
            System.err.println("   Arráncalo desde Servicios de Windows o consola.");
        } catch (Exception e) {
            System.err.println("❌ OTRO ERROR DE RABBITMQ:");
            e.printStackTrace();
        }
    }
}