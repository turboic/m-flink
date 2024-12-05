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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.OnlyExecutionGraphJsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler returning the details for the specified job. */
public class JobDetailsHandler
        extends AbstractAccessExecutionGraphHandler<JobDetailsInfo, JobMessageParameters>
        implements OnlyExecutionGraphJsonArchivist {

    private final MetricFetcher metricFetcher;

    public JobDetailsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobDetailsInfo, JobMessageParameters> messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            MetricFetcher metricFetcher) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);

        this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
    }

    @Override
    protected JobDetailsInfo handleRequest(
            HandlerRequest<EmptyRequestBody> request, AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        return createJobDetailsInfo(executionGraph, metricFetcher);
    }

    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        ResponseBody json = createJobDetailsInfo(graph, null);
        String path =
                getMessageHeaders()
                        .getTargetRestEndpointURL()
                        .replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
        return Collections.singleton(new ArchivedJson(path, json));
    }


    // job详细信息
    private static JobDetailsInfo createJobDetailsInfo(
            AccessExecutionGraph executionGraph, @Nullable MetricFetcher metricFetcher) {

        // 当前时刻
        final long now = System.currentTimeMillis();

        // job初始化时间为开始时间
        final long startTime = executionGraph.getStatusTimestamp(JobStatus.INITIALIZING);

        /**
         * 这段代码的意思是，根据 executionGraph 的当前状态来确定 endTime（结束时间）。具体逻辑如下：
         *
         * 判断是否是全局终止状态：
         *
         * executionGraph.getState().isGloballyTerminalState()：检查 executionGraph 的状态是否是 全局终止状态。isGloballyTerminalState() 是一个返回布尔值的方法，表示当前图的状态是否已经是终止状态（如任务完成、失败或取消等）。
         * 获取状态的时间戳：
         *
         * executionGraph.getStatusTimestamp(executionGraph.getState())：如果状态是终止状态（即全局终止状态），则调用 getStatusTimestamp() 方法，传入当前的状态作为参数，返回该状态发生的时间戳。
         * 默认值：
         *
         * 如果当前状态不是全局终止状态，那么 endTime 被设定为 -1L，表示没有有效的结束时间。
         * 代码解释：
         * 如果当前状态是终止状态，则 endTime 将被设置为该终止状态的时间戳。
         * 如果当前状态不是终止状态，则 endTime 被设置为 -1，表示没有结束时间。
         * 举个例子：
         * 假设 executionGraph 是一个任务执行图。如果该图表示的是一个计算任务流程，executionGraph.getState() 会返回任务的当前状态（如运行中、完成、失败等）。isGloballyTerminalState() 会检查任务是否已经终止，如果终止，则 getStatusTimestamp() 会返回任务结束的时间戳。如果任务还在执行中，则返回 -1，表示任务没有结束。
         *
         * 代码示例：
         * java
         * final long endTime =
         *     executionGraph.getState().isGloballyTerminalState()
         *         ? executionGraph.getStatusTimestamp(executionGraph.getState())
         *         : -1L;
         * executionGraph.getState().isGloballyTerminalState()：判断 executionGraph 是否处于终止状态。
         * executionGraph.getStatusTimestamp(executionGraph.getState())：获取终止状态的时间戳。
         * -1L：如果不是终止状态，设置 endTime 为 -1，表示没有有效的结束时间。
         */
        final long endTime =
                executionGraph.getState().isGloballyTerminalState()
                        ? executionGraph.getStatusTimestamp(executionGraph.getState())
                        : -1L;

        final long duration = (endTime > 0L ? endTime : now) - startTime;

        final Map<JobStatus, Long> timestamps =
                CollectionUtil.newHashMapWithExpectedSize(JobStatus.values().length);

        for (JobStatus jobStatus : JobStatus.values()) {
            timestamps.put(jobStatus, executionGraph.getStatusTimestamp(jobStatus));
        }

        Collection<JobDetailsInfo.JobVertexDetailsInfo> jobVertexInfos =
                new ArrayList<>(executionGraph.getAllVertices().size());

        int[] jobVerticesPerState = new int[ExecutionState.values().length];

        for (AccessExecutionJobVertex accessExecutionJobVertex :
                executionGraph.getVerticesTopologically()) {

            //创建JobVertexDetailsInfo
            final JobDetailsInfo.JobVertexDetailsInfo vertexDetailsInfo =
                    createJobVertexDetailsInfo(
                            accessExecutionJobVertex,
                            now,
                            executionGraph.getJobID(),
                            metricFetcher);

            jobVertexInfos.add(vertexDetailsInfo);
            jobVerticesPerState[vertexDetailsInfo.getExecutionState().ordinal()]++;
        }

        Map<ExecutionState, Integer> jobVerticesPerStateMap =
                CollectionUtil.newHashMapWithExpectedSize(ExecutionState.values().length);

        for (ExecutionState executionState : ExecutionState.values()) {
            jobVerticesPerStateMap.put(
                    executionState, jobVerticesPerState[executionState.ordinal()]);
        }

        // JobDetailsInfo所需要的参数
        return new JobDetailsInfo(
                executionGraph.getJobID(),
                executionGraph.getJobName(),
                executionGraph.isStoppable(),
                executionGraph.getState(),
                executionGraph.getJobType(),
                startTime,
                endTime,
                duration,
                executionGraph.getArchivedExecutionConfig().getMaxParallelism(),
                now,
                timestamps,
                jobVertexInfos,
                jobVerticesPerStateMap,
                new JobPlanInfo.RawJson(executionGraph.getJsonPlan()));
    }

    /**
     * 创建并返回一个包含作业顶点详细信息的对象（JobVertexDetailsInfo）。
     *
     * @param ejv            当前的访问执行作业顶点（AccessExecutionJobVertex），包含了该顶点相关的执行任务信息。
     * @param now            当前时间，用于计算作业的持续时间（如果作业尚未完成）。
     * @param jobId          当前作业的 ID，用于生成作业相关的指标。
     * @param metricFetcher  用于获取指标的工具类，提供 IO 和其他相关的性能指标。
     * @return               返回一个 JobVertexDetailsInfo 对象，包含了作业顶点的详细信息。
     */
    private static JobDetailsInfo.JobVertexDetailsInfo createJobVertexDetailsInfo(
            AccessExecutionJobVertex ejv, long now, JobID jobId, MetricFetcher metricFetcher) {

        // 创建一个数组，用于记录不同执行状态下的任务数量。
        int[] tasksPerState = new int[ExecutionState.values().length];

        // 初始化开始时间为最大值，结束时间为最小值。
        long startTime = Long.MAX_VALUE;
        long endTime = 0;

        // 默认假设所有任务都已完成。
        boolean allFinished = true;

        // 遍历当前作业顶点的所有执行任务
        for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
            final ExecutionState state = vertex.getExecutionState();
            tasksPerState[state.ordinal()]++;  // 统计该状态下的任务数量

            // 记录该任务的部署状态时间戳，找出最早的开始时间
            long started = vertex.getStateTimestamp(ExecutionState.DEPLOYING);
            if (started > 0L) {
                startTime = Math.min(startTime, started);  // 更新最早的开始时间
            }

            // 更新是否所有任务都已完成
            allFinished &= state.isTerminal();

            // 更新任务的结束时间（基于状态时间戳）
            endTime = Math.max(endTime, vertex.getStateTimestamp(state));
        }

        // 计算作业的持续时间
        long duration;
        if (startTime < Long.MAX_VALUE) {  // 如果有有效的开始时间
            if (allFinished) {
                duration = endTime - startTime;  // 如果所有任务都完成，计算结束时间与开始时间的差
            } else {
                endTime = -1L;  // 如果任务未完成，结束时间设为 -1
                duration = now - startTime;  // 持续时间为当前时间与开始时间的差
            }
        } else {
            startTime = -1L;  // 如果没有开始时间，则设为 -1
            endTime = -1L;    // 结束时间也设为 -1
            duration = -1L;   // 持续时间设为 -1
        }

        // 计算当前作业顶点的汇总状态（基于子任务状态）
        ExecutionState jobVertexState =
                ExecutionJobVertex.getAggregateJobVertexState(tasksPerState, ejv.getParallelism());

        // 创建一个任务状态的映射，记录各状态下的任务数量
        Map<ExecutionState, Integer> tasksPerStateMap =
                CollectionUtil.newHashMapWithExpectedSize(tasksPerState.length);

        for (ExecutionState executionState : ExecutionState.values()) {
            tasksPerStateMap.put(executionState, tasksPerState[executionState.ordinal()]);
        }

        // 创建一个 I/O 指标对象，用于记录 I/O 操作的相关数据
        MutableIOMetrics counts = new MutableIOMetrics();

        // 遍历当前作业顶点的所有执行任务，收集每个任务的 I/O 指标
        for (AccessExecutionVertex vertex : ejv.getTaskVertices()) {
            // 使用当前执行尝试的指标代表子任务的指标（不汇总所有尝试的指标）
            counts.addIOMetrics(
                    vertex.getCurrentExecutionAttempt(),
                    metricFetcher,
                    jobId.toString(),
                    ejv.getJobVertexId().toString());
        }

        // 创建最终的 IOMetricsInfo 对象，包含所有收集到的 I/O 指标数据
        final IOMetricsInfo jobVertexMetrics =
                new IOMetricsInfo(
                        counts.getNumBytesIn(),
                        counts.isNumBytesInComplete(),
                        counts.getNumBytesOut(),
                        counts.isNumBytesOutComplete(),
                        counts.getNumRecordsIn(),
                        counts.isNumRecordsInComplete(),
                        counts.getNumRecordsOut(),
                        counts.isNumRecordsOutComplete(),
                        counts.getAccumulateBackPressuredTime(),
                        counts.getAccumulateIdleTime(),
                        counts.getAccumulateBusyTime());

        // 返回一个包含作业顶点详细信息的对象
        return new JobDetailsInfo.JobVertexDetailsInfo(
                ejv.getJobVertexId(),                         // 作业顶点 ID
                ejv.getSlotSharingGroup().getSlotSharingGroupId(), // 作业顶点的 SlotSharingGroup ID
                ejv.getName(),                                // 作业顶点名称
                ejv.getMaxParallelism(),                      // 最大并行度
                ejv.getParallelism(),                         // 当前并行度
                jobVertexState,                               // 汇总后的作业顶点状态
                startTime,                                    // 最早的开始时间
                endTime,                                      // 结束时间
                duration,                                     // 作业的持续时间
                tasksPerStateMap,                             // 各个任务状态的数量映射
                jobVertexMetrics);                           // I/O 指标
    }
}
