package flinckcdc.sink;


import com.alibaba.fastjson.JSONObject;
import flinckcdc.TestBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<String> {

    Connection connection = null;
    PreparedStatement insertSmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        String url = "jdbc:mysql://hd02:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false";
        connection = DriverManager.getConnection(url,"root","");
        insertSmt = connection.prepareStatement("REPLACE into test2(id,name) values (?,?)");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

        System.err.println(value);
        JSONObject jsonObject = JSONObject.parseObject(value);
        System.out.println(jsonObject.get("afterData"));

        TestBean afterData = JSONObject.parseObject(JSONObject.toJSONString(jsonObject.get("afterData")), TestBean.class);

        //直接执行更新语句
        insertSmt.setInt(1,afterData.getId());
        insertSmt.setString(2,afterData.getName());
        insertSmt.execute();
    }

    @Override
    public void close() throws Exception {
        insertSmt.close();
        connection.close();
    }


}


