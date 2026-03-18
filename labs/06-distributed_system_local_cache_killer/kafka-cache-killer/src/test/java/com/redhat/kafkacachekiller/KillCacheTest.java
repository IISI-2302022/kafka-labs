package com.redhat.kafkacachekiller;

import com.redhat.cleanbase.test.annotation.MvcInfo;
import com.redhat.cleanbase.test.base.BaseMvcTest;
import com.redhat.kafkacachekiller.status.Status;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.web.bind.annotation.RequestMethod;

@EmbeddedKafka(count = 3, topics = "${spring.kafka.template.default-topic}", brokerProperties = "auto.create.topics.enable = ${spring.kafka.admin.auto-create:true}", partitions = 1, kraft = true)
public class KillCacheTest extends BaseMvcTest {

    @DynamicPropertySource
    public static void setup(DynamicPropertyRegistry registry) {
        registry.add("spring_kafka_bootstrap_servers", () -> "localhost:9092");
        registry.add("cache_kill_wait_time_enabled", () -> false);
    }

    @MvcInfo(url = "/cache/kill", httpMethod = RequestMethod.POST)
    @Test
    public void triggerKillCache() throws Exception {
        val perform = mvc.perform(getMockRequestBuilder());
        perform.andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("status").value(Status.ASYNC.name()));
        Thread.sleep(1000);
    }

}
