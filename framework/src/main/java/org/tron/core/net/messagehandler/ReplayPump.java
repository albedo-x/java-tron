package org.tron.core.net.messagehandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.overlay.message.Message;
import org.tron.core.net.TronNetService;
import org.tron.core.net.peer.PeerConnection;

/**
 * Stores one message type, then sends the stored batch to a random peer.
 * Each handler keeps its own pump. A queue size of 0 or a speed of 0
 * logs the old failure line and does not send.
 */
@Slf4j(topic = "net")
final class ReplayPump {

  private final long msgQueueMaxSize;
  private final long msgMaxSpeed;
  private volatile Map<Object, Message> msgGathered = new HashMap<>();
  private volatile Map<Object, Message> msgQueue = new HashMap<>();
  private final long[] msgTotalSent = {0};
  private final long[] msgSuccessSent = {0};
  private final long[] tmpTimestamp = {0};
  volatile long cnt;

  ReplayPump(long msgQueueMaxSize, long msgMaxSpeed) {
    this.msgQueueMaxSize = msgQueueMaxSize;
    this.msgMaxSpeed = msgMaxSpeed;
    new Thread(() -> {
      if (msgQueueMaxSize == 0 || msgMaxSpeed == 0) {
        logger.info(
            "@@@ msgQueueMaxSize or msgMaxSpeed was not found in config file. task failed..");
        return;
      }
      while (true) {
        try {
          logger.info("@@@ msgQueue size {}, peers {}",
              msgQueue.size(), TronNetService.getPeers().size());
          if (msgQueue.size() >= msgQueueMaxSize && TronNetService.getPeers().size() >= 1) {
            tmpTimestamp[0] = System.currentTimeMillis();
            msgQueue.values().forEach(v -> {
              try {
                msgTotalSent[0]++;
                if (msgTotalSent[0] % msgMaxSpeed == 0) {
                  long sleepMs = tmpTimestamp[0] + 1000 - System.currentTimeMillis();
                  logger.info("&&& total send {}, success count {}, sleep {}ms",
                      msgTotalSent[0], msgSuccessSent[0], sleepMs);
                  if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                  }
                  tmpTimestamp[0] = System.currentTimeMillis();
                  msgSuccessSent[0] = 0;
                }
              } catch (Exception ignored) {
                // The old attack node swallowed a sleep interrupt and kept sending.
              }
              List<PeerConnection> list = TronNetService.getPeers();
              if (list.size() == 0) {
                logger.info("@@@ peer size == 0 return...");
                return;
              }
              int index = new Random().nextInt(list.size());
              PeerConnection peerConnection = list.get(index);
              peerConnection.sendMessage(v);
              msgSuccessSent[0]++;
            });
          } else {
            Thread.sleep(1000);
          }
        } catch (Exception e) {
          logger.error("@@@ adv msg failed.", e);
          try {
            Thread.sleep(1000);
          } catch (Exception ignored) {
            // The old attack node swallowed a sleep interrupt and kept the loop.
          }
        }
      }
    }).start();
  }

  void offer(Object key, Message msg) {
    if (msgQueueMaxSize > 0 && msgMaxSpeed > 0) {
      msgGathered.put(key, msg);
      cnt++;
      logger.info("### cnt = {}, map1-size = {}, map2-size = {}",
          cnt, msgGathered.size(), msgQueue.size());
      if (msgGathered.size() >= msgQueueMaxSize) {
        msgQueue = msgGathered;
        msgGathered = new HashMap<>();
      }
    }
  }
}
