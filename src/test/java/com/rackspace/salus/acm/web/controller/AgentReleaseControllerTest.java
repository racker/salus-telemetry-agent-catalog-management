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

package com.rackspace.salus.acm.web.controller;

import static com.rackspace.salus.test.JsonTestUtils.readContent;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.rackspace.salus.acm.services.AgentReleaseService;
import com.rackspace.salus.acm.web.model.AgentReleaseCreate;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.repositories.AgentReleaseRepository;
import com.rackspace.salus.test.JsonTestUtils;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@WebMvcTest(controllers = AgentReleaseController.class)
public class AgentReleaseControllerTest {

  @Autowired
  MockMvc mockMvc;

  @MockBean
  AgentReleaseRepository agentReleaseRepository;

  @MockBean
  AgentReleaseService agentReleaseService;

  @Test
  public void testGetAgentReleasesForTenant() throws Exception {
    final AgentRelease release = populateRelease();

    when(agentReleaseRepository.findAll(any()))
        .thenReturn(
            pageOfSingleton(release)
        );

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/agent-releases?page=0&size=1",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_release_response_paged.json"), true));

    verify(agentReleaseRepository).findAll(PageRequest.of(0, 1));

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  @Test
  public void testGetAgentReleaseForTenant() throws Exception {
    final AgentRelease release = populateRelease();

    when(agentReleaseRepository.findById(any()))
        .thenReturn(Optional.of(release));

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/agent-releases/{agentReleaseId}",
        "t-1", "00000000-0000-0000-0001-000000000000"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_release_response.json"), true));

    verify(agentReleaseRepository).findById(UUID.fromString("00000000-0000-0000-0001-000000000000"));

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  @Test
  public void testGetAgentReleases() throws Exception {
    final AgentRelease release = populateRelease();

    when(agentReleaseRepository.findAll(any()))
        .thenReturn(
            pageOfSingleton(release)
        );

    mockMvc.perform(get(
        "/api/admin/agent-releases?page=0&size=1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_release_response_paged.json"), true));

    verify(agentReleaseRepository).findAll(PageRequest.of(0, 1));

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  @Test
  public void testGetAgentRelease() throws Exception {
    final AgentRelease release = populateRelease();

    when(agentReleaseRepository.findById(any()))
        .thenReturn(Optional.of(release));

    mockMvc.perform(get(
        "/api/admin/agent-releases/{agentReleaseId}",
        "00000000-0000-0000-0001-000000000000"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_release_response.json"), true));

    verify(agentReleaseRepository).findById(UUID.fromString("00000000-0000-0000-0001-000000000000"));

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  @Test
  public void testDeclareAgentRelease() throws Exception {
    final AgentRelease release = populateRelease();

    when(agentReleaseService.create(any()))
        .thenReturn(release);

    mockMvc.perform(
        post("/api/admin/agent-releases")
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                JsonTestUtils.readContent("AgentInstallControllerTest/agent_release_create.json")))
        .andExpect(status().isCreated())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_release_response.json"), true));

    verify(agentReleaseService).create(new AgentReleaseCreate()
        .setType(AgentType.TELEGRAF)
        .setVersion("1.11.0")
        .setLabels(singletonMap("os", "linux"))
        .setUrl("https://dl.influxdata.com/telegraf/releases/telegraf-1.11.0-static_linux_amd64.tar.gz")
        .setExe("./telegraf/telegraf")
    );

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  @Test
  public void testDelete() throws Exception {
    final AgentRelease release = populateRelease();

    mockMvc.perform(
        delete("/api/admin/agent-releases/{agentReleaseId}", release.getId()))
        .andExpect(status().isNoContent());

    verify(agentReleaseService).delete(release.getId());

    verifyNoMoreInteractions(
        agentReleaseRepository, agentReleaseService);
  }

  private AgentRelease populateRelease() {
    return new AgentRelease()
        .setId(UUID.fromString("00000000-0000-0000-0001-000000000000"))
        .setType(AgentType.TELEGRAF)
        .setLabels(singletonMap("os", "linux"))
        .setVersion("1.11.0")
        .setUrl(
            "https://dl.influxdata.com/telegraf/releases/telegraf-1.11.0-static_linux_amd64.tar.gz")
        .setExe("./telegraf/telegraf")
        .setCreatedTimestamp(Instant.ofEpochSecond(100000))
        .setUpdatedTimestamp(Instant.ofEpochSecond(100001));
  }

  private <T> Page<T> pageOfSingleton(T value) {
    return new PageImpl<>(Collections.singletonList(value));
  }

}