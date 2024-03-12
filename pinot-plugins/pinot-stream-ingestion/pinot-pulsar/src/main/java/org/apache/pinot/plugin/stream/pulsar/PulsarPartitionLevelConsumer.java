/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.pulsar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PartitionGroupConsumer} implementation for the Pulsar stream
 */
public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  private final ExecutorService _executorService;
  private final ScheduledExecutorService _scheduledExecutorService;
  private final Reader _reader;
  private boolean _enableKeyValueStitch;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    super(clientId, streamConfig);
    PulsarConfig config = new PulsarConfig(streamConfig, clientId);
    _reader =
        createReaderForPartition(config.getPulsarTopicName(), partitionGroupConsumptionStatus.getPartitionGroupId(),
            config.getInitialMessageId());
    LOGGER.info("Created pulsar reader with id {} for topic {} partition {}", _reader, _config.getPulsarTopicName(),
        partitionGroupConsumptionStatus.getPartitionGroupId());
    _executorService = Executors.newSingleThreadExecutor();
    _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    _enableKeyValueStitch = _config.getEnableKeyValueStitch();
  }

  /**
   * Fetch records from the Pulsar stream between the start and end StreamPartitionMsgOffset
   * Used {@link org.apache.pulsar.client.api.Reader} to read the messaged from pulsar partitioned topic
   * The reader seeks to the startMsgOffset and starts reading records in a loop until endMsgOffset or timeout is
   * reached.
   */
  @Override
  public PulsarMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset,
      StreamPartitionMsgOffset endMsgOffset, int timeoutMillis) {
    final MessageId startMessageId = ((MessageIdStreamOffset) startMsgOffset).getMessageId();
    final MessageId endMessageId =
        endMsgOffset == null ? MessageId.latest : ((MessageIdStreamOffset) endMsgOffset).getMessageId();

    final BlockingQueue<PulsarStreamMessage> messageQueue = new LinkedBlockingQueue<>();
    final AtomicBoolean timeoutReached = new AtomicBoolean(false);
    final AtomicReference<CompletableFuture<Boolean>> pulsarHasMessagesFutureRef = new AtomicReference<>();
    Future<?> pulsarResultFuture =
        _executorService.submit(() -> fetchMessages(startMessageId, endMessageId, messageQueue::add, timeoutReached,
            pulsarHasMessagesFutureRef));
    //schedule a thread at the fetch timeout time to cleanly stop the pulsar read.
    _scheduledExecutorService.schedule(() -> {
      timeoutReached.set(true);
      if (pulsarHasMessagesFutureRef.get() != null) {
        CompletableFuture<Boolean> hasMessageFuture = pulsarHasMessagesFutureRef.get();
        if (!hasMessageFuture.isDone()) {
          hasMessageFuture.completeExceptionally(
              new TimeoutException("Timeout while waiting for message availability"));
        }
      }
    }, timeoutMillis, TimeUnit.MILLISECONDS);

    try {
      pulsarResultFuture.get();
    } catch (Exception e) {
      // The fetchMessages has thrown an exception. Most common cause is the timeout.
      // We return the records fetched till now along with the next start offset.
      boolean cancelSuccess = pulsarResultFuture.cancel(true);
      if (!cancelSuccess) {
        LOGGER.warn("Failed to cancel the fetchMessages task");
      }
      LOGGER.warn("Error while fetching records from Pulsar", e);
    }
    LOGGER.debug("Fetched {} records from Pulsar", messageQueue.size());
    List<PulsarStreamMessage> messageList = new ArrayList<>(messageQueue.size());
    messageQueue.drainTo(messageList);
    messageList.removeIf(message -> !messageIsValidAndInRange(message, startMessageId, endMessageId));
    LOGGER.debug("Creating batch of {} records.", messageList.size());

    return new PulsarMessageBatch(messageList, _enableKeyValueStitch);
  }

  public void fetchMessages(MessageId startMessageId, MessageId endMessageId,
      Consumer<PulsarStreamMessage> messageConsumer, AtomicBoolean timeoutReached,
      AtomicReference<CompletableFuture<Boolean>> pulsarHasMessagesFuture) {
    try {
      _reader.seek(startMessageId);

      while (!timeoutReached.get()) {
        CompletableFuture<Boolean> hasMessageFuture = _reader.hasMessageAvailableAsync();
        pulsarHasMessagesFuture.set(hasMessageFuture);
        try {
          if (!hasMessageFuture.get()) {
            break;
          }
        } catch (Exception e) {
          LOGGER.warn("Error while waiting for message availability.", e);
          break;
        }
        Message<byte[]> nextMessage = _reader.readNext();

        if (endMessageId != null) {
          if (nextMessage.getMessageId().compareTo(endMessageId) > 0) {
            break;
          }
        }
        messageConsumer.accept(
            PulsarUtils.buildPulsarStreamMessage(nextMessage, _enableKeyValueStitch, _pulsarMetadataExtractor));

        if (Thread.interrupted()) {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Error consuming records from Pulsar topic", e);
    }
  }

  private boolean messageIsValidAndInRange(PulsarStreamMessage message, final MessageId startOffset,
      final MessageId endOffset) {
    return message != null && message.getValue() != null
        && (message.getMessageId().compareTo(startOffset) >= 0)
        && (endOffset == null) || (message.getMessageId().compareTo(endOffset) < 0);
  }
  @Override
  public void close()
      throws IOException {
    _reader.close();
    super.close();
    shutdownAndAwaitTermination(_scheduledExecutorService);
    shutdownAndAwaitTermination(_executorService);
  }

  void shutdownAndAwaitTermination(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
