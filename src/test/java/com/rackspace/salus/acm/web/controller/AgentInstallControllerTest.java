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

import com.rackspace.salus.telemetry.entities.AgentInstall;
import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.entities.BoundAgentInstall;
import com.rackspace.salus.telemetry.repositories.AgentInstallRepository;
import com.rackspace.salus.telemetry.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.acm.services.AgentInstallService;
import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.test.JsonTestUtils;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@WebMvcTest(controllers = AgentInstallController.class)
@AutoConfigureDataJpa
public class AgentInstallControllerTest {

  @Autowired
  MockMvc mockMvc;

  @MockBean
  AgentInstallRepository agentInstallRepository;

  @MockBean
  BoundAgentInstallRepository boundAgentInstallRepository;

  @MockBean
  AgentInstallService agentInstallService;

  @Test
  public void getBindingForResourceAndAgentType_found() throws Exception {

    final AgentRelease release = populateRelease();
    final AgentInstall install = populateInstall(release);

    when(boundAgentInstallRepository.findAllByTenantResourceAgentType(any(), any(), any()))
        .thenReturn(Collections.singletonList(
            new BoundAgentInstall()
                .setResourceId("r-1")
                .setAgentInstall(install)
        ));

    mockMvc.perform(get(
        "/api/admin/bound-agent-installs/{tenantId}/{resourceId}/{agentType}",
        "t-1", "r-1", "TELEGRAF"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/single_install_binding.json"), true));

    verify(boundAgentInstallRepository).findAllByTenantResourceAgentType(
        "t-1", "r-1", AgentType.TELEGRAF
    );

    verifyNoMoreInteractions(
        boundAgentInstallRepository, agentInstallRepository, agentInstallService);
  }

  @Test
  public void testGetAgentInstalls() throws Exception {
    final AgentRelease release = populateRelease();
    final AgentInstall install = populateInstall(release);

    when(agentInstallRepository.findAllByTenantId(any(), any()))
        .thenReturn(
            pageOfSingleton(install)
        );

    mockMvc.perform(get(
        "/api/tenant/{tenantId}/agent-installs?page=0&size=1",
        "t-1"
    ).accept(MediaType.APPLICATION_JSON))
        .andExpect(status().isOk())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_install_response_paged.json"), true));

    verify(agentInstallRepository).findAllByTenantId("t-1", PageRequest.of(0, 1));

    verifyNoMoreInteractions(
        boundAgentInstallRepository, agentInstallRepository, agentInstallService);
  }

  @Test
  public void testCreate() throws Exception {
    final AgentRelease release = populateRelease();
    final AgentInstall install = populateInstall(release);

    when(agentInstallService.install(any(), any()))
        .thenReturn(install);

    mockMvc.perform(
        post("/api/tenant/{tenantId}/agent-installs", "t-1")
            .accept(MediaType.APPLICATION_JSON)
            .contentType(MediaType.APPLICATION_JSON)
            .content(
                JsonTestUtils.readContent("AgentInstallControllerTest/agent_install_create.json")))
        .andExpect(status().isCreated())
        .andExpect(content().json(
            // id field should not be returned
            readContent("AgentInstallControllerTest/agent_install_response.json"), true));

    verify(agentInstallService).install("t-1", new AgentInstallCreate()
        .setAgentReleaseId(release.getId())
        .setLabelSelector(Collections.singletonMap("os", "linux"))
    );

    verifyNoMoreInteractions(
        boundAgentInstallRepository, agentInstallRepository, agentInstallService);
  }

  @Test
  public void testDelete() throws Exception {
    final AgentRelease release = populateRelease();
    final AgentInstall install = populateInstall(release);

    mockMvc.perform(
        delete(
            "/api/tenant/{tenantId}/agent-installs/{agentInstallId}",
            "t-1",
            "00000000-0000-0000-0002-000000000000"))
        .andExpect(status().isNoContent());

    verify(agentInstallService).delete("t-1", UUID.fromString("00000000-0000-0000-0002-000000000000"));

    verifyNoMoreInteractions(
        boundAgentInstallRepository, agentInstallRepository, agentInstallService);
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

  private AgentInstall populateInstall(AgentRelease release) {
    return new AgentInstall()
        .setId(UUID.fromString("00000000-0000-0000-0002-000000000000"))
        .setLabelSelector(singletonMap("os", "linux"))
        .setTenantId("t-1")
        .setCreatedTimestamp(Instant.ofEpochSecond(100002))
        .setUpdatedTimestamp(Instant.ofEpochSecond(100003))
        .setAgentRelease(release);
  }

  private <T> Page<T> pageOfSingleton(T value) {
    return new PageImpl<>(Collections.singletonList(value));
  }
}