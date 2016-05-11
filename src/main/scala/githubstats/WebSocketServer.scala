package githubstats

import java.net.InetSocketAddress
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake

import scala.collection.JavaConverters._

/**
 * A websocket server that manages connections and broadcasts data.
 */
class WebSocketServer(port: Int) extends org.java_websocket.server.WebSocketServer(new InetSocketAddress(port)) {
  
  override def onOpen(conn: WebSocket, handshake: ClientHandshake) {
  }
  
  override def onClose(conn: WebSocket, code: Int, reason: String, remote: Boolean) {
  }
  
  override def onError(conn: WebSocket, exception: Exception) {
    throw exception;
  }
  
  override def onMessage(conn: WebSocket, message: String) {
    println(s"Websocket message: $message") 
  }
  
  /**
   * Send a message to all connected clients.
   */
  def send(count: Any){
    connections.asScala.foreach(_.send(s"$count"))
  }

}