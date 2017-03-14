import com.ehl.phoenix.sql.util.Callback;
import com.ehl.phoenix.sql.util.PhoenixSqlUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

/**
 * Created by 雷晓武 on 2017/3/8.
 */
public class Example {

    public static void main(String []args) throws Exception {
            final long start = System.nanoTime();
        PhoenixSqlUtils phoenixSqlUtils = PhoenixSqlUtils.getInstance();
        phoenixSqlUtils.execute(new Callback<Object>() {
            @Override
            public Object call(Connection conn) throws Exception {
                System.out.println("=="+(System.nanoTime()-start));
                ResultSet rs = conn.createStatement().executeQuery("select * from \"mobile\""+" LIMIT 5 OFFSET 10");

                while (rs.next()){
                    System.out.println(rs.getString(1)+"\t"+rs.getLong(2)+"\t"+rs.getString(3)+"\t"+rs.getLong(5)+"\t"+rs.getLong(6));
                }
                rs.close();

//                pst.close();
                System.out.println("==111"+(System.nanoTime()-start));
                return null;
            }
        });
    }
}
