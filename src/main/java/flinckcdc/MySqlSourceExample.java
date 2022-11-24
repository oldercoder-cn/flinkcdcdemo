package flinckcdc;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import flinckcdc.sink.HiveSink;
import flinckcdc.sink.MysqlSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

//https://blog.csdn.net/haoheiao/article/details/126519588
public class MySqlSourceExample {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hd02")
                .port(3306)
                .databaseList("testdb") // set captured database
                .tableList("testdb.testtable") // set captured table
                .scanNewlyAddedTableEnabled(true)
                .username("root")
                .password("")
                //.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                //自定义反序列化器
                .deserializer(new FlinkCdcDataDeserializationSchema())
                //latest:新增的,earliest:历史数据,initial:初始化数据？
                .startupOptions(StartupOptions.latest())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(1000);

//        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                // set 4 parallel source tasks
//                .setParallelism(4)
//                .print("==>").setParallelism(1); // use parallelism 1 for sink to keep message ordering


        //使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");


        //打印数据
        mysqlDS.printToErr("------>").setParallelism(1);


        //写入数据到MYSQL TEST2表
        mysqlDS.addSink(new MysqlSink());

        //写入到HIVE
        mysqlDS.addSink(new HiveSink());


        //6.执行任务
        env.execute("FlinkCDC_mysql");
    }
}
