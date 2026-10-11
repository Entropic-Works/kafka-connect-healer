package e2e;

import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.util.concurrent.Executors;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

// Test-only HTTPS gateway. Successful requests go to the real Connect worker.
public class FaultProxy {
    public static void main(String[] arguments) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(Path.of("/opt/kafka/e2e-tls/connect.p12"))) {
            keys.load(input, "test-only-password".toCharArray());
        }
        KeyManagerFactory manager = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        manager.init(keys, "test-only-password".toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(manager.getKeyManagers(), null, null);
        HttpsServer server = HttpsServer.create(new InetSocketAddress(8444), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(tls));
        server.setExecutor(Executors.newFixedThreadPool(8));
        HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().toString();
            String method = exchange.getRequestMethod();
            String mode = Files.exists(Path.of("/data/proxy-mode")) ? Files.readString(Path.of("/data/proxy-mode")).trim() : "pass";
            boolean restart = method.equals("POST") && path.contains("/restart");
            boolean statuses = method.equals("GET") && path.equals("/connectors?expand=status");
            Files.writeString(Path.of("/data/proxy-requests"), System.currentTimeMillis() + " " + method + " " + path + " " + mode + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            try {
                byte[] body;
                int code;
                if ((restart && mode.startsWith("restart-")) || (statuses && mode.startsWith("status-"))) {
                    String fault = mode.substring(mode.indexOf('-') + 1);
                    code = 200;
                    body = "{}".getBytes();
                    switch (fault) {
                        case "malformed": body = "{not valid JSON".getBytes(); break;
                        case "null": body = "null".getBytes(); break;
                        case "missing": body = "{\"task\":{}}".getBytes(); break;
                        case "empty": break;
                        case "delay": Thread.sleep(6000); body = "{}".getBytes(); break;
                        case "disconnect": exchange.close(); return;
                        case "redirect": code = 302; exchange.getResponseHeaders().set("Location", "/redirect-target"); break;
                        default: code = Integer.parseInt(fault); break;
                    }
                } else {
                    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:8083" + path))
                        .timeout(java.time.Duration.ofSeconds(10))
                        .method(method, HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()));
                    for (String header : new String[]{"Authorization", "Content-Type"}) {
                        String value = exchange.getRequestHeaders().getFirst(header);
                        if (value != null) request.header(header, value);
                    }
                    var response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
                    code = response.statusCode();
                    body = response.body();
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(code, code == 204 ? -1 : body.length);
                if (code != 204) exchange.getResponseBody().write(body);
            } catch (Exception failure) {
                System.err.println("test gateway: " + failure.getClass().getSimpleName());
            } finally {
                exchange.close();
            }
        });
        server.start();
        System.out.println("Test fault gateway listening on 8444");
    }
}
