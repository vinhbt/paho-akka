package com.sandinh.paho.akka.publish

import akka.actor.{Actor, ActorRef}
import com.sandinh.paho.akka.{PSConfig, Publish}
import org.eclipse.paho.client.mqttv3._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

class MqttPublish(cfg: PSConfig) extends Actor {

  private val logger = org.log4s.getLogger

  //++++ internal Actor event messages ++++//
  private case object Init
  private case class Connect(id: Int)
  private case class Connected(id: Int)
  private case class Disconnected(id: Int)

  private val queues: Seq[MClient] = (0 until cfg.numPublishInstance).map { id => client(id) }

  private var balanceIndex: Int = 0

  //use to stash the pub-sub messages when disconnected
  //note that we do NOT store the sender() in to the stash as in akka.actor.StashSupport#theStash
  private[this] val pubStash = mutable.Queue.empty[(Long, Publish)]

  class MClient(var connectCount: Int, val client: IMqttAsyncClient, val conn: ConnListener, var state: MqttState)

  class ConnListener(owner: ActorRef, id: Int) extends IMqttActionListener {
    def onSuccess(asyncActionToken: IMqttToken): Unit = {
      owner ! Connected(id)
    }

    def onFailure(asyncActionToken: IMqttToken, e: Throwable): Unit = {
      owner ! Disconnected(id)
    }
  }

  private class PubSubMqttCallback(owner: ActorRef, id: Int) extends MqttCallback {
    def connectionLost(cause: Throwable): Unit = {
      owner ! Disconnected(id)
    }
    /** only logging */
    def deliveryComplete(token: IMqttDeliveryToken): Unit = {
    }
    def messageArrived(topic: String, message: MqttMessage): Unit = {
    }
  }

  //setup MqttAsyncClient without MqttClientPersistence
  private def client(id: Int): MClient = {
    val c = new MqttAsyncClient(cfg.brokerUrl, s"${MqttAsyncClient.generateClientId()}-$id", null)
    c.setCallback(new PubSubMqttCallback(self, id))
    new MClient(0, c, new ConnListener(self, id), DisconnectedState)
  }

  private def connect(id: Int): Unit = {
    //only receive Connect when client.isConnected == false so its safe here to call client.connect
    val c = queues(id)
    try {
      logger.info(s"connecting to ${cfg.brokerUrl}..")
      queues(id).state = ConnectingState
      c.connectCount += 1
      c.client.connect(cfg.conOpt, null, c.conn)
    } catch {
      case e: Exception =>
        logger.error(e)(s"can't connect to $cfg")
        c.state = DisconnectedState
        delayConnect(id)
    }
  }

  private def delayConnect(id: Int): Unit = {
    if (queues(id).state == DisconnectedState) {
      val delay = cfg.connectDelay(queues(id).connectCount)
      logger.info(s"delay $delay before reconnect")
      context.system.scheduler.scheduleOnce(delay, self, Connect(id))
      queues(id).state = ConnectingState
    }
  }

  self ! Init

  private def addToStash(m: Publish): Unit = {
    if (pubStash.length > cfg.stashCapacity)
      while (pubStash.length > cfg.stashCapacity / 2) pubStash.dequeue()
    pubStash += (System.nanoTime -> m)
  }

  private def sendMsg(m: Publish): Unit = {
    var num = 0
    while (queues(balanceIndex).state != ConnectedState && num < cfg.numPublishInstance) {
      num = num + 1
      balanceIndex = balanceIndex + 1
      if (balanceIndex >= cfg.numPublishInstance) balanceIndex = 0
    }
    if (queues(balanceIndex).state == ConnectedState) {
      try {
        queues(balanceIndex).client.publish(m.topic, m.message())
      } catch {
        case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_MAX_INFLIGHT =>
          addToStash(m)
        case e: MqttException if e.getReasonCode == MqttException.REASON_CODE_CLIENT_NOT_CONNECTED =>
          queues(balanceIndex).state == DisconnectedState
          self ! Disconnected(balanceIndex)
        case e: MqttException =>
          logger.info(s"can't publish to ${m.topic} ${e.getReasonCode}")
        case e: Throwable =>
          logger.info(s"can't publish to ${m.topic} ${e.getMessage}")
      }
    } else {
      addToStash(m)
    }
  }

  def receive = {
    case m: Publish      => sendMsg(m)
    case m: Disconnected => delayConnect(m.id)
    case m: Connect      => connect(m.id)
    case m: Connected =>
      queues(m.id).state = ConnectedState
      queues(m.id).connectCount = 0
      //remove expired Publish messages
      if (cfg.stashTimeToLive.isFinite())
        pubStash.dequeueAll(_._1 + cfg.stashTimeToLive.toNanos < System.nanoTime)
      while (pubStash.nonEmpty) self ! pubStash.dequeue()._2
    case Init => (0 until cfg.numPublishInstance).foreach(id => connect(id))
  }

}
