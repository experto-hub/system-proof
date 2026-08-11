package io.github.jacekkardys.systemproof.examples.ingestion;

import org.springframework.http.MediaType;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SmsIngestionController {
    private final SmsAcknowledgementStrategy acknowledgementStrategy;

    public SmsIngestionController(SmsAcknowledgementStrategy acknowledgementStrategy) {
        this.acknowledgementStrategy = acknowledgementStrategy;
    }

    @PostMapping(
        path = "/v1/ingestion/sms",
        consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces = MediaType.TEXT_PLAIN_VALUE
    )
    public String ingest(@RequestParam MultiValueMap<String, String> form) {
        acknowledgementStrategy.ingest(JasminSmsCallback.from(form).toCommand());
        return "ACK/Jasmin";
    }
}
