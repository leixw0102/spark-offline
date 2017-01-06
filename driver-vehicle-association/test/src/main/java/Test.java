import com.google.inject.AbstractModule;
import com.google.inject.Guice;

/**
 * Created by 雷晓武 on 2017/1/4.
 */
public class Test {

    public String test(){
        return "leixw";
    }

    public static void main(String[]args){
        Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {

            }
        });
    }
}
