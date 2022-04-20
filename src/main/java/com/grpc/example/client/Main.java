package com.grpc.example.client;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.grpc.example.PingExample.ClientFactory;
import com.grpc.example.PingExample.PingExampleClient;
import com.grpc.example.PingExample.PingMetadata;
import com.grpc.example.PingExampleGrpc;
import com.grpc.example.PingRequest;
import com.grpc.example.PingResponse;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.okhttp.OkHttpChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.perfmark.PerfMark;
import io.perfmark.traceviewer.TraceEventViewer;
import io.servicetalk.grpc.netty.GrpcClients;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final int NUMBER_OF_RPC = 1000;

  private static final Metadata.Key<String> TRACE_SEQ_KEY =
      Metadata.Key.of("trace-seq", ASCII_STRING_MARSHALLER);

  private Main() {}

  public static void main(final String... args) throws Exception {
    switch (System.getProperty("com.grpc.example.transport", "netty")) {
      case "okhttp":
        logger.info("Using okhttp");
        nativeGrpc(OkHttpChannelBuilder.forAddress("127.0.0.1", 50051).usePlaintext().build());
        break;
      case "servicetalk":
        logger.info("Using servicetalk");
        serviceTalkGrpc();
        break;
      default:
        logger.info("Using netty");
        nativeGrpc(NettyChannelBuilder.forAddress("127.0.0.1", 50051).usePlaintext().build());
    }
  }

  private static void forceConnect(final ManagedChannel channel) {
    while (true) {
      var state = channel.getState(true);
      if (ConnectivityState.READY == state) {
        break;
      }
    }
  }

  private static void forceConnect(final PingExampleClient client) throws Exception {
    client
        .asBlockingClient()
        .ping(new PingMetadata(Duration.ofSeconds(1)), PingRequest.newBuilder().build());
  }

  private static void nativeGrpc(final ManagedChannel channel)
      throws IOException, InterruptedException {
    PerfMark.setEnabled(true);
    forceConnect(channel);

    var stub = PingExampleGrpc.newStub(channel);
    var slowCalls = new AtomicInteger(0);
    var countDownLatch = new CountDownLatch(NUMBER_OF_RPC);

    for (int i = 0; i < NUMBER_OF_RPC; i++) {
      var started = Stopwatch.createStarted();
      var seqStr = String.valueOf(i);

      logger.info("starting call with trace-seq: {}", seqStr);

      var metadata = new Metadata();
      metadata.put(TRACE_SEQ_KEY, seqStr);
      stub.withDeadlineAfter(100, TimeUnit.MILLISECONDS)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
          .ping(
              PingRequest.newBuilder().build(),
              new StreamObserver<PingResponse>() {
                @Override
                public void onNext(PingResponse pingResponse) {}

                @Override
                public void onError(Throwable throwable) {
                  countDownLatch.countDown();
                  started.stop();
                  var status = Status.fromThrowable(throwable);
                  logger.info("{} Status code: {}", seqStr, status.getCode());
                  logger.info("{} Status description: {}", seqStr, status.getDescription());
                  if (Code.DEADLINE_EXCEEDED == status.getCode()) {
                    slowCalls.incrementAndGet();
                  }
                }

                @Override
                public void onCompleted() {
                  countDownLatch.countDown();
                  started.stop();
                  var elapsed = started.elapsed(TimeUnit.MILLISECONDS);
                  logger.info("{} - call latency {} ms", seqStr, elapsed);
                }
              });
    }

    awaitTermination(
        () -> {
          channel.shutdown();
          try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            logger.error("Interrupted closing native gRPC client", e);
            Thread.currentThread().interrupt();
          }
        },
        countDownLatch,
        slowCalls);
    TraceEventViewer.writeTraceHtml();
  }

  private static void serviceTalkGrpc() throws Exception {
    var client =
        GrpcClients.forAddress("127.0.0.1", 50051)
            .defaultTimeout(Duration.ofMillis(100))
            .build(new ClientFactory());
    var slowCalls = new AtomicInteger(0);
    var countDownLatch = new CountDownLatch(NUMBER_OF_RPC);

    forceConnect(client);

    for (int i = 0; i < NUMBER_OF_RPC; i++) {
      var started = Stopwatch.createStarted();
      var seqStr = String.valueOf(i);

      logger.info("starting call with trace-seq: {}", seqStr);

      client
          .ping(PingRequest.newBuilder().build())
          .whenOnError(
              throwable -> {
                countDownLatch.countDown();
                started.stop();
                if (Throwables.getRootCause(throwable) instanceof TimeoutException) {
                  slowCalls.incrementAndGet();
                }
              })
          .whenOnSuccess(
              ignored -> {
                countDownLatch.countDown();
                started.stop();
                var elapsed = started.elapsed(TimeUnit.MILLISECONDS);
                logger.info("{} - call latency {} ms", seqStr, elapsed);
              })
          .toFuture();
    }

    awaitTermination(
        () -> {
          try {
            client.closeGracefully();
          } catch (Exception e) {
            logger.error("failed to close servicetalk client gracefully", e);
          }
        },
        countDownLatch,
        slowCalls);
  }

  private static void awaitTermination(
      final Runnable shutdownFn, final CountDownLatch countDownLatch, final AtomicInteger slowCalls)
      throws InterruptedException {
    var completed = countDownLatch.await(60, TimeUnit.SECONDS);
    if (!completed) {
      logger.error("termination timed out");
    }
    shutdownFn.run();
    logger.info("{} calls didn't finish within the deadline", slowCalls.get());
  }
}
