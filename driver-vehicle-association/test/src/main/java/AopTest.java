import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class AopTest {
    static class MyInterceptor implements MethodInterceptor {
        public Object invoke(MethodInvocation invocation) throws Throwable {
            System.out.println("before.");
            Object returnData = invocation.proceed();
            System.out.println("after.");
            return returnData;
        }
    }
    static class MyModule implements Module {
        public void configure(Binder binder) {
            Matcher m = Matchers.any();
            binder.bindInterceptor(m, Matchers.any(), new MyInterceptor());
        }
    }
    //
    public void foo() {
        System.out.println("Hello Word.");
    }
    //
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new MyModule());
        Test obj = injector.getInstance(Test.class);
        obj.test();
    }
}