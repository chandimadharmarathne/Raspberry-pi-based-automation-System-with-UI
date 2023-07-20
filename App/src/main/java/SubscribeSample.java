import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SubscribeSample {
    private static final String MQTT_BROKER = "tcp://test.mosquitto.org:1883";
    private static final String MQTT_TOPIC = "payload";
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/mqttvalues";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "123";

    public static void main(String[] args) {
        try {
            MqttClient client = new MqttClient(MQTT_BROKER, MqttClient.generateClientId(), new MemoryPersistence());
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable throwable) {
                    System.out.println("Connection to MQTT broker lost!");
                }

                public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    if (MQTT_TOPIC.equals(s)) {
                        String payload = new String(mqttMessage.getPayload());
                        System.out.println("Received payload: " + payload);
                        insertPayloadIntoDatabase(payload);
                    }
                }

                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                    // Not used in this example
                }
            });

            client.connect();
            client.subscribe(MQTT_TOPIC);
            System.out.println("Connected to MQTT broker");

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private static void insertPayloadIntoDatabase(String payload) {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            String sql = "INSERT INTO sensor_data (temperature, humidity, date_time) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            JSONObject jsonPayload = new JSONObject(payload);
            preparedStatement.setDouble(1, jsonPayload.getDouble("temperature"));
            preparedStatement.setInt(2, jsonPayload.getInt("humidity"));

            // Convert the date_time string to a java.sql.Timestamp object
            String dateTimeString = jsonPayload.getString("date_time");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parsedDate = dateFormat.parse(dateTimeString);
            Timestamp timestamp = new Timestamp(parsedDate.getTime());

            preparedStatement.setTimestamp(3, timestamp);

            preparedStatement.executeUpdate();
            System.out.println("Payload inserted into the database");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
