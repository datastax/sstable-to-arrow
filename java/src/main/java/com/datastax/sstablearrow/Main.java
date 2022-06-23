package com.datastax.sstablearrow;

import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;

import javax.ws.rs.core.Application;

public class Main {
    public static void main(String[] args) {
        Application application = new ReloadController();
        NettyJaxrsServer server = new NettyJaxrsServer();
        server.getDeployment().setApplication(application);
        server.setRootResourcePath("/");

        server.setPort(8080);
        server.getDeployment().getApplication().toString();
        server.start();
    }
}
