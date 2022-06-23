package com.datastax.sstablearrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.cassandra.schema.TableMetadata;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import javax.ws.rs.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.nio.file.Path;
import java.util.*;

@ApplicationPath("/api")
public class ReloadController extends Application {
    private Set<Object> singletons = new HashSet<>();

    public ReloadController() {
        super();
        singletons.add(new ReloadService());
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}

