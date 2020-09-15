/*
 * Copyright 2019 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.acm.services;

import com.rackspace.salus.common.config.MetricNames;
import com.rackspace.salus.common.config.MetricTags;
import com.rackspace.salus.common.config.MetricTagValues;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import com.rackspace.salus.acm.web.model.AgentReleaseCreate;
import com.rackspace.salus.telemetry.errors.AlreadyExistsException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AgentReleaseService {

  private final AgentReleaseRepository repository;

  MeterRegistry meterRegistry;

  // metrics counters
  private final Counter.Builder agentReleaseSuccess;

  @Autowired
  public AgentReleaseService(AgentReleaseRepository repository, MeterRegistry meterRegistry) {
    this.repository = repository;
    this.meterRegistry = meterRegistry;
    this.agentReleaseSuccess = Counter.builder(MetricNames.SERVICE_OPERATION_SUCCEEDED)
        .tag(MetricTags.SERVICE_METRIC_TAG,"AgentRelease");
  }

  public AgentRelease create(AgentReleaseCreate in) {

    final List<AgentRelease> existing = repository
        .findAllByTypeAndVersion(in.getType(), in.getVersion());
    for (AgentRelease agentRelease : existing) {
      if (agentRelease.getLabels().equals(in.getLabels())) {
        throw new AlreadyExistsException(
            "An agent release with same type, version, and labels already exists");
      }
    }

    final AgentRelease agentRelease = new AgentRelease()
        .setType(in.getType())
        .setVersion(in.getVersion())
        .setLabels(in.getLabels())
        .setUrl(in.getUrl())
        .setExe(in.getExe());

    final AgentRelease saved = repository
        .save(agentRelease);

    log.info("Created agentRelease={}", saved);
    agentReleaseSuccess
        .tags(MetricTags.OPERATION_METRIC_TAG, MetricTagValues.CREATE_OPERATION,MetricTags.OBJECT_TYPE_METRIC_TAG,"agentRelease")
        .register(meterRegistry).increment();
    return saved;
  }

  public void delete(UUID agentReleaseId) {
    log.info("Deleting agentReleaseId={}", agentReleaseId);
    repository.deleteById(agentReleaseId);
    agentReleaseSuccess
        .tags(MetricTags.OPERATION_METRIC_TAG, MetricTagValues.REMOVE_OPERATION,MetricTags.OBJECT_TYPE_METRIC_TAG,"agentRelease")
        .register(meterRegistry).increment();
  }
}
