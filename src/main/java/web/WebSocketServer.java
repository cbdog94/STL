package web;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SocketServer
 */
@ServerEndpoint("/init")
public class WebSocketServer {

    private static final Map<String, WebSocketServer> connections = new ConcurrentHashMap<>();

    private Session session;

    public WebSocketServer() {

    }

    @OnOpen
    public void onOpen(Session session) {
        try {
            this.session = session;

            session.getBasicRemote().sendText("openWebSocket...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @OnMessage
    public void onMessage(String message) {
        System.out.println(message);
        connections.put(message, this);
//        try {
//            session.getBasicRemote().sendText(message);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @OnClose
    public void onClose(Session session) {

    }

    public static void sendMessage(String id, String message) {

        try {
            connections.get(id).session.getBasicRemote().sendText(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
