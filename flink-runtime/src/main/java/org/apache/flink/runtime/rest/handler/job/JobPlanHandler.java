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

import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.OnlyExecutionGraphJsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

/** Handler serving the job execution plan. */
/**
 * 处理请求并返回作业执行计划的 REST 请求处理器。
 * 该处理器用于提供作业的执行计划信息，通常用于作业管理和监控系统中。
 *
 * 继承自 {@link AbstractAccessExecutionGraphHandler}，用于处理与作业执行图（ExecutionGraph）相关的请求。
 * 该处理器实现了 {@link OnlyExecutionGraphJsonArchivist}，允许将作业执行计划的 JSON 信息归档。
 */
public class JobPlanHandler
        extends AbstractAccessExecutionGraphHandler<JobPlanInfo, JobMessageParameters>
        implements OnlyExecutionGraphJsonArchivist {

    /**
     * 构造函数，初始化 JobPlanHandler。
     *
     * @param leaderRetriever      用于检索集群领导者（RESTful 网关）的工具。
     * @param timeout             请求超时时间。
     * @param headers             请求头部信息。
     * @param messageHeaders      请求和响应消息头部信息。
     * @param executionGraphCache 执行图缓存，存储和提供访问作业执行图的方法。
     * @param executor            用于异步执行任务的线程池或执行器。
     */
    public JobPlanHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> headers,
            MessageHeaders<EmptyRequestBody, JobPlanInfo, JobMessageParameters> messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor) {

        // 调用父类构造方法初始化
        super(leaderRetriever, timeout, headers, messageHeaders, executionGraphCache, executor);
    }

    /**
     * 处理 HTTP 请求，返回作业的执行计划信息。
     *
     * @param request 请求对象，包含请求的参数。
     * @param executionGraph 当前作业的执行图。
     * @return 返回一个 JobPlanInfo 对象，包含作业的执行计划信息。
     * @throws RestHandlerException 如果在处理请求时出现错误，抛出异常。
     */
    @Override
    protected JobPlanInfo handleRequest(
            HandlerRequest<EmptyRequestBody> request, AccessExecutionGraph executionGraph)
            throws RestHandlerException {
        // 创建并返回作业执行计划信息
        return createJobPlanInfo(executionGraph);
    }

    /**
     * 将作业执行图的 JSON 信息归档，并附加路径信息。
     *
     * @param graph 当前作业的执行图。
     * @return 返回归档的 JSON 信息和目标路径。
     * @throws IOException 如果归档过程中发生错误，抛出异常。
     */
    @Override
    public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph)
            throws IOException {
        // 创建作业执行计划的 JSON 信息
        ResponseBody json = createJobPlanInfo(graph);

        // 生成归档路径，替换 URL 中的 JobID
        String path =
                getMessageHeaders()
                        .getTargetRestEndpointURL()
                        .replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());

        // 返回一个包含归档信息的集合（只有一个元素）
        return Collections.singleton(new ArchivedJson(path, json));
    }

    /**
     * 根据给定的执行图生成作业执行计划信息。
     *
     * @param executionGraph 当前作业的执行图。
     * @return 返回包含执行图 JSON 的 JobPlanInfo 对象。
     */
    private static JobPlanInfo createJobPlanInfo(AccessExecutionGraph executionGraph) {
        // 将执行图转换为 JSON 格式并返回包装在 JobPlanInfo 对象中
        return new JobPlanInfo(executionGraph.getJsonPlan());
    }
}
