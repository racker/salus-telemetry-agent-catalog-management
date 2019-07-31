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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord;
import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.salus.acm.entities.AgentInstall;
import com.rackspace.salus.acm.entities.AgentRelease;
import com.rackspace.salus.acm.entities.BoundAgentInstall;
import com.rackspace.salus.acm.repositories.AgentInstallRepository;
import com.rackspace.salus.acm.repositories.AgentReleaseRepository;
import com.rackspace.salus.acm.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.common.transactions.EnableJpaKafkaTransactions;
import com.rackspace.salus.resource_management.web.client.ResourceApi;
import com.rackspace.salus.resource_management.web.model.ResourceDTO;
import com.rackspace.salus.telemetry.messaging.AgentInstallChangeEvent;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.checkerframework.checker.nullness.Opt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

@RunWith(SpringRunner.class)
@DataJpaTest(showSql = false)
@Import({
    ObjectMapper.class,
    AgentInstallService.class,
})
@ImportAutoConfiguration({
    KafkaAutoConfiguration.class
})
@EmbeddedKafka(topics = {AgentInstallTransactionTest.TEST_TOPIC1, AgentInstallTransactionTest.TEST_TOPIC2},
   brokerProperties = {"transaction.state.log.replication.factor=1",
                       "transaction.state.log.min.isr=1"},
   partitions = 1)
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class AgentInstallTransactionTest {

  public static final String TEST_TOPIC1 = "test1.topic.json";
  public static final String TEST_TOPIC2 = "test2.topic.json";

  static {
    System.setProperty(
        EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers");
  }

  @TestConfiguration
  @EnableJpaKafkaTransactions
  public static class Config {

//    @Bean
//    public ZonesProperties zonesProperties() {
//      return new ZonesProperties();
//    }
//
//    @Bean
//    public ServicesProperties servicesProperties() {
//      return new ServicesProperties()
//          .setResourceManagementUrl("");
//    }
  }

    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    EntityManager entityManager;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @MockBean
    BoundEventSender mockEventSender;

    BoundEventSender boundEventSender;

    private PodamFactory podamFactory = new PodamFactoryImpl();

    // IntelliJ gets confused finding this broker bean when @SpringBootTest is activated
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    private String tenantId;

    private Consumer<String, AgentInstallChangeEvent> consumer;
    private DefaultKafkaConsumerFactory<String, AgentInstallChangeEvent> consumerFactory;

    private AgentInstallCreate create;

    @Autowired
    AgentInstallService agentInstallService;

    @Autowired
    AgentInstallRepository agentInstallRepository;
    @Autowired
    BoundAgentInstallRepository boundAgentInstallRepository;

    @Autowired
    AgentReleaseRepository agentReleaseRepository;

    @MockBean
    ResourceApi resourceApi;

    private ResourceDTO dummyResource;

    @Autowired
  EntityManagerFactory entityManagerFactory;
    private AgentRelease agentRelease;




    @Before
    //@Transactional
    public void setUp() {
      tenantId = RandomStringUtils.randomAlphanumeric(10);
      create = podamFactory.manufacturePojo(AgentInstallCreate.class);
      agentRelease = podamFactory.manufacturePojo(AgentRelease.class);
      AgentRelease ar2 =  agentReleaseRepository.save(agentRelease);
      create.setAgentReleaseId(ar2.getId());

      //  entityManager.flush();
     // Optional<AgentRelease> agentRelease1 = agentReleaseRepository.findById(agentRelease.getId());
     // System.out.println(ar2);
//      entityManagerFactory.createEntityManager().flush();

      dummyResource = podamFactory.manufacturePojo(ResourceDTO.class);
      dummyResource.setAssociatedWithEnvoy(true);
      List<ResourceDTO> resources = new ArrayList<>();
      resources.add(dummyResource);
      when(resourceApi.getResourcesWithLabels(anyString(), any())).thenReturn(resources);

      final Map<String, Object> consumerProps = KafkaTestUtils
          .consumerProps("testAgentInstallTransaction", "true", embeddedKafka);
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      consumerProps
          .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProps
          .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
      consumerProps
          .put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

      consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(),
          new JsonDeserializer<>(AgentInstallChangeEvent.class));

      consumer = consumerFactory.createConsumer();
    }

    @After
    public void finish() {
//      consumer.close();
    }

    @Test
    @Transactional(propagation = NOT_SUPPORTED)
    public void testAgentInstallTransaction() {
      String testTopic = TEST_TOPIC1;
      KafkaTopicProperties properties = new KafkaTopicProperties();
      properties.setInstalls(testTopic);
      boundEventSender = new BoundEventSender(kafkaTemplate, properties);

      doAnswer(invocation -> {
//        boundEventSender.sendTo(invocation.getArgument(0), invocation.getArgument(1),
//            invocation.getArgument(2));
        return null;
      })
          .when(mockEventSender).sendTo(any(), any(), any());


      agentInstallService.install(tenantId, create);
      Iterator<AgentInstall> agentInstallIterator = agentInstallRepository.findAll().iterator();
      AgentInstall agentInstall = agentInstallIterator.next();
      Assert.assertEquals(agentInstall.getAgentRelease().getId(), create.getAgentReleaseId());
      Assert.assertEquals(agentInstall.getTenantId(), tenantId);
      Assert.assertEquals(agentInstallIterator.hasNext(), false);

      Iterator<BoundAgentInstall> boundAgentInstallIterator = boundAgentInstallRepository.findAll()
          .iterator();
      BoundAgentInstall b = boundAgentInstallIterator.next();
      Assert.assertEquals(b.getResourceId(), dummyResource.getResourceId());
      Assert.assertEquals(b.getAgentInstall().getTenantId(), tenantId);
      Assert.assertEquals(boundAgentInstallIterator.hasNext(), false);

//      embeddedKafka.consumeFromEmbeddedTopics(consumer, testTopic);
//      ConsumerRecord<String, AgentInstallChangeEvent> record = getSingleRecord(consumer, testTopic,
//          5000);
//      Assert.assertEquals(record.value().getResourceId(), dummyResource.getResourceId());

    }

//  @Test
//  @Transactional(propagation = NOT_SUPPORTED)
//  public void testAgentInstallTransactionWithException() {
//    String testTopic = TEST_TOPIC2;
//
//    RuntimeException runtimeException = new RuntimeException("very scary");
//    KafkaTopicProperties properties = new KafkaTopicProperties();
//    properties.setMonitors(testTopic);
//    monitorEventProducer = new MonitorEventProducer(kafkaTemplate, properties);
//
//    //  This throws the exception after both repo's are saved and the kafka event has been sent
//    doAnswer(invocation -> {monitorEventProducer.sendMonitorEvent(invocation.getArgument(0));
//                            throw runtimeException;})
//        .when(mockEventProducer).sendMonitorEvent(any());
//    try {
//      monitorManagement.createMonitor(tenantId, create);
//    } catch(Exception e) {
//      Assert.assertEquals(e, runtimeException);
//    }
//
//
//    Iterator<Monitor> monitorIterator = monitorRepository.findAll().iterator();
//    Assert.assertEquals(monitorIterator.hasNext(), false);
//
//    Iterator<BoundMonitor> boundMonitorIterator = boundMonitorRepository.findAll().iterator();
//    Assert.assertEquals(boundMonitorIterator.hasNext(), false);
//
//    embeddedKafka.consumeFromEmbeddedTopics(consumer, testTopic);
//    ConsumerRecords<String, MonitorBoundEvent> records = getRecords(consumer, 5000);
//    Iterator<ConsumerRecord<String, MonitorBoundEvent>> consumerRecordIterator = records.iterator();
//    Assert.assertEquals(consumerRecordIterator.hasNext(), false);
//  }
//

}