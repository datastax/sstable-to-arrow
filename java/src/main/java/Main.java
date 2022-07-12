import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.core.Application;

import com.datastax.cndb.bulkimport.BulkImporterHttpResource;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;

public class Main extends Application
{

    private Set<Object> singletons = new HashSet<>();

    public Main() {
        super();
        singletons.add(new BulkImporterHttpResource());
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    public static void init()
    {
        System.setProperty("cassandra.config", "file:///" + System.getProperty("user.dir") + "/src/main/resources/cassandra.yaml");

        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.loadSchema();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    public static void main(String[] args) throws IOException
    {
        init();

        Application application = new Main();
        NettyJaxrsServer server = new NettyJaxrsServer();
        server.getDeployment().setApplication(application);
        server.setRootResourcePath("/");

        server.setPort(8080);
        server.getDeployment().getApplication().toString();
        server.start();
    }
}