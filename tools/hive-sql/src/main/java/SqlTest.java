import java.sql.*;

/**
 * Created by 雷晓武 on 2017/3/21.
 */
public class SqlTest {

    public static void main(String[]args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con=null;
        PreparedStatement sta=null;
        ResultSet result=null;
        ResultSetMetaData t = null;
        try{
            con= DriverManager.getConnection("jdbc:hive2://10.150.27.242:10000/default","hive","");
            sta= con.prepareStatement("select field1,count(*) from test05 group by field1 limit 5");
//            result = sta.executeQuery();
//              t  = result.getMetaData();
//            int count = t.getColumnCount();
//            StringBuffer sb = new StringBuffer();

//            System.out.println(sb.toString());
            result = sta.executeQuery();
            t  = result.getMetaData();
            while(result.next()){
                for(int i=1;i<t.getColumnCount();i++){
                    System.out.print(result.getString(i)+"\t");
                }
                System.out.println("\r\n");

            }
        } catch(SQLException e) {
            e.printStackTrace();
        }finally {
            if(null != result){
                result.close();
            }
            if(null != sta){
                sta.close();
            }
            if(null != con){
                con.close();
            }

        }
    }

}
