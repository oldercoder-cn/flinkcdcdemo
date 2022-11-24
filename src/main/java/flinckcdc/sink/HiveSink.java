package flinckcdc.sink;

import com.alibaba.fastjson.JSONObject;
import flinckcdc.TestBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * 自定义 Hive Sink  Hive的建表语句
 CREATE TABLE test.hivetable (
 id VARCHAR(100),
 name VARCHAR(100)
 );
 ;
 */
public class HiveSink extends RichSinkFunction<String> {
    private PreparedStatement pstm;
    private Connection conn;
    private String sql="insert into test.hivetable(id,name) values(?,?)";

    public HiveSink() {
    }
//    public HiveSink(String sql) {
//        this.sql = sql;
//    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = getConnection();
        pstm = conn.prepareStatement(sql);

    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        System.out.println(value);

        JSONObject jsonObject = JSONObject.parseObject(value);
        System.out.println(jsonObject.get("afterData"));

        TestBean afterData = JSONObject.parseObject(JSONObject.toJSONString(jsonObject.get("afterData")), TestBean.class);

        //直接执行更新语句
        pstm.setInt(1,afterData.getId());
        pstm.setString(2,afterData.getName());
        pstm.execute();
//        for (int i = 1; i <= value.size(); i++) {
//            pstm.setString(i, value.get(i - 1));
//        }
//        pstm.executeUpdate();

    }

    @Override
    public void close() {
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {

            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static Connection getConnection() {
        Connection conn = null;
        try {
            String jdbc = "org.apache.hive.jdbc.HiveDriver";
            String url = "jdbc:hive2://hd02:10000/test";
            String user = "";
            String password = "";
            Class.forName(jdbc);
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}
