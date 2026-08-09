package io.github.jacekkardys.systemproof.examples.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.util.LinkedMultiValueMap;

@SpringBootTest(
    classes = SystemProofIngestionApplication.class,
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
        "system-proof.ingestion.acknowledgement-mode=before-commit",
        "management.endpoint.health.group.readiness.include=readinessState",
        "spring.autoconfigure.exclude="
            + "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,"
            + "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration"
    }
)
class EarlyAcknowledgementControllerHttpTest {
    private static final String ENDPOINT = "/v1/ingestion/sms";

    @Autowired
    TestRestTemplate http;

    @MockitoBean
    SmsIngestionService service;

    @Test
    void returnsThePositiveAcknowledgementBeforeTheBoundedIngestionTaskCompletes()
        throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            started.countDown();
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out awaiting controlled ingestion release");
            }
            completed.countDown();
            return null;
        }).when(service).ingest(any());

        var response = postSms();

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("ACK/Jasmin");
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(completed.getCount()).isOne();

        release.countDown();
        assertThat(completed.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private org.springframework.http.ResponseEntity<String> postSms() {
        var form = new LinkedMultiValueMap<String, String>();
        form.add("id", "jasmin-message-early-ack");
        form.add("from", "48111000111");
        form.add("to", "99001");
        form.add("content", "test message");
        form.add("coding", "0");
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        return http.exchange(
            ENDPOINT,
            HttpMethod.POST,
            new HttpEntity<>(form, headers),
            String.class
        );
    }
}
