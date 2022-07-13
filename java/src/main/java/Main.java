import javax.ws.rs.core.Application;

import com.datastax.cndb.bulkimport.BulkImporterApplication;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;

public class Main
{

    public static void main(String[] args)
    {
        BulkImporterApplication.init();
        Application application = new BulkImporterApplication();
        NettyJaxrsServer server = new NettyJaxrsServer();
        server.getDeployment().setApplication(application);
        server.setRootResourcePath("/");

        server.setPort(8080);
        server.getDeployment().getApplication().toString();
        server.start();
    }
}