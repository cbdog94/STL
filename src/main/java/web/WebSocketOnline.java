package web;

import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Online SocketServer
 *
 * @author Bin Cheng
 */
@ServerEndpoint("/online/{userID}")
public class WebSocketOnline {

    private static Map<String, Session> clients = new ConcurrentHashMap<>();

    @OnOpen
    public void onOpen(@PathParam("userID") String userID, Session session) {
        clients.put(userID, session);
    }

    static void sendMessage(String id, String message) {
        try {
            clients.get(id).getBasicRemote().sendText(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
