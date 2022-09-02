import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import org.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;


public class KafkaProductConsumer {
    public static void main(String[] args) {


        Connection conn=null;
        Statement stmt =null;

        KafkaConsumer consumer;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "test");
        consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("productdb4"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String Data = record.value();
                System.out.println(record.value());

               JSONObject jsonObject = new JSONObject(record.value());
                System.out.println(String.valueOf(jsonObject));
                String getModelName = String.valueOf(jsonObject.getString("modelName"));
                String getReleaseYear = String.valueOf(jsonObject.getInt("release_year"));
                String getBrand = String.valueOf(jsonObject.getString("brand"));
                String getPrice = String.valueOf(jsonObject.getInt("price"));
                String getSellerName = String.valueOf(jsonObject.getString("seller_name"));
                String getColors = String.valueOf(jsonObject.getString("colours"));
                String getDate = String.valueOf((jsonObject.getString("manufacture_date")));
                System.out.println(getModelName);
                System.out.println(getReleaseYear);
                System.out.println(getBrand);
                System.out.println(getPrice);
                System.out.println(getSellerName);
                System.out.println(getColors);
                System.out.println(getDate);



                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/mobileshop", "root", "");
                    System.out.println("connection created");
                    stmt = (Statement) conn.createStatement();
//                    String querry ="INSERT INTO `temperature`(`tempertaure`) VALUES ("+getTemp+")";
//                    String querry = "INSERT INTO `temperature`(`tempertaure`, `humidity`) VALUES (" + getTemp + "," + getHum + ")";
                    String querry= "INSERT INTO `products`(`model`, `release_year`, `brand`, `price`, `seller_name`, `color`, `manufacture_date`) VALUES ('" + getModelName + "'," + getReleaseYear + ",'"+getBrand+"',"+getPrice+",'"+getSellerName+"','"+getColors+"','"+getDate+"')";
                    System.out.println(querry);
                    stmt.executeUpdate(querry);
                    System.out.println("Temperature is: " + Data);


                } catch (Exception e) {
                    System.out.println("check connection!!!!");
                    System.out.println(e);
                }
            }
        }

    }
}
