/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Heartbeat manager implementation. The heartbeat manager maintains a map of heartbeat monitors and
 * resource IDs. Each monitor will be updated when a new heartbeat of the associated machine has
 * been received. If the monitor detects that a heartbeat has timed out, it will notify the {@link
 * HeartbeatListener} about it. A heartbeat times out iff no heartbeat signal has been received
 * within a given timeout interval.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
@ThreadSafe
class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O> {

    /** Heartbeat timeout interval in milli seconds. */
    private final long heartbeatTimeoutIntervalMs;

    private final int failedRpcRequestsUntilUnreachable;

    /** Resource ID which is used to mark one own's heartbeat signals. */
    private final ResourceID ownResourceID;

    /** Heartbeat listener with which the heartbeat manager has been associated. */
    private final HeartbeatListener<I, O> heartbeatListener;

    /** Executor service used to run heartbeat timeout notifications. */
    private final ScheduledExecutor mainThreadExecutor;

    protected final Logger log;

    /** Map containing the heartbeat monitors associated with the respective resource ID. */
    private final ConcurrentHashMap<ResourceID, HeartbeatMonitor<O>> heartbeatTargets;

    private final HeartbeatMonitor.Factory<O> heartbeatMonitorFactory;

    /** Running state of the heartbeat manager 心跳管理的运行状态. */
    protected volatile boolean stopped;

    public HeartbeatManagerImpl(
            long heartbeatTimeoutIntervalMs,
            int failedRpcRequestsUntilUnreachable,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {
        this(
                heartbeatTimeoutIntervalMs,
                failedRpcRequestsUntilUnreachable,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                new DefaultHeartbeatMonitor.Factory<>());
    }

    public HeartbeatManagerImpl(
            long heartbeatTimeoutIntervalMs,
            int failedRpcRequestsUntilUnreachable,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log,
            HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {

        Preconditions.checkArgument(
                heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
        this.failedRpcRequestsUntilUnreachable = failedRpcRequestsUntilUnreachable;
        this.ownResourceID = Preconditions.checkNotNull(ownResourceID);
        this.heartbeatListener = Preconditions.checkNotNull(heartbeatListener, "heartbeatListener");
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
        this.log = Preconditions.checkNotNull(log);
        this.heartbeatMonitorFactory = heartbeatMonitorFactory;
        this.heartbeatTargets = new ConcurrentHashMap<>(16);

        stopped = false;
    }

    // ----------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------

    ResourceID getOwnResourceID() {
        return ownResourceID;
    }

    HeartbeatListener<I, O> getHeartbeatListener() {
        return heartbeatListener;
    }

    Map<ResourceID, HeartbeatMonitor<O>> getHeartbeatTargets() {
        return heartbeatTargets;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatManager methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget) {
        if (!stopped) {
            if (heartbeatTargets.containsKey(resourceID)) {
                log.debug(
                        "The target with resource ID {} is already been monitored.",
                        resourceID.getStringWithMetadata());
            } else {
                HeartbeatMonitor<O> heartbeatMonitor =
                        heartbeatMonitorFactory.createHeartbeatMonitor(
                                resourceID,
                                heartbeatTarget,
                                mainThreadExecutor,
                                heartbeatListener,
                                heartbeatTimeoutIntervalMs,
                                failedRpcRequestsUntilUnreachable);

                heartbeatTargets.put(resourceID, heartbeatMonitor);

                // check if we have stopped in the meantime (concurrent stop operation)
                if (stopped) {
                    heartbeatMonitor.cancel();

                    heartbeatTargets.remove(resourceID);
                }
            }
        }
    }

    @Override
    public void unmonitorTarget(ResourceID resourceID) {
        if (!stopped) {
            //从缓存中移除
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.remove(resourceID);

            if (heartbeatMonitor != null) {
                //自身执行退出
                heartbeatMonitor.cancel();
            }
        }
    }

    @Override
    public void stop() {
        stopped = true;

        for (HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {
            heartbeatMonitor.cancel();
        }

        heartbeatTargets.clear();
    }

    @Override
    public long getLastHeartbeatFrom(ResourceID resourceId) {
        HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceId);

        if (heartbeatMonitor != null) {
            return heartbeatMonitor.getLastHeartbeat();
        } else {
            return -1L;
        }
    }

    ScheduledExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatTarget methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> receiveHeartbeat(
            ResourceID heartbeatOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat from {}.", heartbeatOrigin);
            reportHeartbeat(heartbeatOrigin);

            if (heartbeatPayload != null) {
                heartbeatListener.reportPayload(heartbeatOrigin, heartbeatPayload);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    @Override
    public CompletableFuture<Void> requestHeartbeat(
            final ResourceID requestOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat request from {}.", requestOrigin);

            final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(requestOrigin);

            if (heartbeatTarget != null) {
                if (heartbeatPayload != null) {
                    heartbeatListener.reportPayload(requestOrigin, heartbeatPayload);
                }

                heartbeatTarget
                        .receiveHeartbeat(
                                getOwnResourceID(),
                                heartbeatListener.retrievePayload(requestOrigin))
                        .whenCompleteAsync(handleHeartbeatRpc(requestOrigin), mainThreadExecutor);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    protected BiConsumer<Void, Throwable> handleHeartbeatRpc(ResourceID heartbeatTarget) {
        return (unused, failure) -> {
            if (failure != null) {
                handleHeartbeatRpcFailure(
                        heartbeatTarget, ExceptionUtils.stripCompletionException(failure));
            } else {
                handleHeartbeatRpcSuccess(heartbeatTarget);
            }
        };
    }

    private void handleHeartbeatRpcSuccess(ResourceID heartbeatTarget) {
        runIfHeartbeatMonitorExists(heartbeatTarget, HeartbeatMonitor::reportHeartbeatRpcSuccess);
    }

    private void handleHeartbeatRpcFailure(ResourceID heartbeatTarget, Throwable failure) {
        if (failure instanceof RecipientUnreachableException) {
            reportHeartbeatTargetUnreachable(heartbeatTarget);
        }
    }

    private void reportHeartbeatTargetUnreachable(ResourceID heartbeatTarget) {
        runIfHeartbeatMonitorExists(heartbeatTarget, HeartbeatMonitor::reportHeartbeatRpcFailure);
    }

    private void runIfHeartbeatMonitorExists(
            ResourceID heartbeatTarget, Consumer<? super HeartbeatMonitor<?>> action) {
        final HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(heartbeatTarget);

        if (heartbeatMonitor != null) {
            action.accept(heartbeatMonitor);
        }
    }

    HeartbeatTarget<O> reportHeartbeat(ResourceID resourceID) {
        if (heartbeatTargets.containsKey(resourceID)) {
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(resourceID);
            heartbeatMonitor.reportHeartbeat();

            return heartbeatMonitor.getHeartbeatTarget();
        } else {
            return null;
        }
    }
}
