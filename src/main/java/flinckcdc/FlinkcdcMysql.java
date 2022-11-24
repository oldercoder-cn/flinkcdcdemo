package flinckcdc;

//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.DebeziumSourceFunction;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;


//import com.ververica.cdc.connectors.mysql.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class FlinkcdcMysql {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//
//
//        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
//                .hostname("openlookeng")
//                .port(3306)
//                .databaseList("flinkcdc-1") // set captured database
//                .tableList("flinkcdc-1.test") // set captured table
//                .username("root")
//                .password("123456")
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .startupOptions(StartupOptions.initial())
//                .build();
//
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
//
//        env.addSource(sourceFunction)
//                .print()
//                .setParallelism(1);
//
//        env.execute();
    }
}
