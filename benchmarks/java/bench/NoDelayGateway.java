package bench;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import javax.net.ServerSocketFactory;
import py4j.GatewayServer;

/**
 * Py4J's gateway server with TCP_NODELAY set on every connection it accepts.
 *
 * <p>Py4J's own server leaves Nagle's algorithm on. A value of 8 KB or more that it returns goes
 * out as more than one write, the last of which waits for the client's delayed acknowledgement of
 * the ones before: about 40 ms a call. This runs the same server through a socket factory that
 * turns the algorithm off, so that the comparison can be read without that wait.
 */
public final class NoDelayGateway {

  /** A server socket whose accepted connections have TCP_NODELAY set. */
  private static final class NoDelayServerSocket extends ServerSocket {
    NoDelayServerSocket() throws IOException {}

    @Override
    public Socket accept() throws IOException {
      Socket socket = super.accept();
      socket.setTcpNoDelay(true);
      return socket;
    }
  }

  private static final class Factory extends ServerSocketFactory {
    // The one Py4J calls: it binds the socket itself
    @Override
    public ServerSocket createServerSocket() throws IOException {
      return new NoDelayServerSocket();
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
      ServerSocket socket = createServerSocket();
      socket.bind(new java.net.InetSocketAddress(port));
      return socket;
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog) throws IOException {
      ServerSocket socket = createServerSocket();
      socket.bind(new java.net.InetSocketAddress(port), backlog);
      return socket;
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog, InetAddress address)
        throws IOException {
      ServerSocket socket = createServerSocket();
      socket.bind(new java.net.InetSocketAddress(address, port), backlog);
      return socket;
    }
  }

  public static void main(String[] args) {
    int port = Integer.parseInt(args[0]);
    GatewayServer server =
        new GatewayServer.GatewayServerBuilder()
            .javaPort(port)
            .serverSocketFactory(new Factory())
            .build();
    server.start();
  }
}
