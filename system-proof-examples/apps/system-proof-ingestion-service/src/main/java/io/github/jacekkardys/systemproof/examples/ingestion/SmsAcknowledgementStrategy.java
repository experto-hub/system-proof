package io.github.jacekkardys.systemproof.examples.ingestion;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

interface SmsAcknowledgementStrategy {
    String MODE_PROPERTY = "system-proof.ingestion.acknowledgement-mode";

    void ingest(SmsIngestionCommand command);
}

@Component
@ConditionalOnProperty(
    name = SmsAcknowledgementStrategy.MODE_PROPERTY,
    havingValue = "after-commit",
    matchIfMissing = true
)
final class CommitBeforeAcknowledgement implements SmsAcknowledgementStrategy {
    private final SmsIngestionService service;

    CommitBeforeAcknowledgement(SmsIngestionService service) {
        this.service = service;
    }

    @Override
    public void ingest(SmsIngestionCommand command) {
        service.ingest(command);
    }
}

@Component
@ConditionalOnProperty(
    name = SmsAcknowledgementStrategy.MODE_PROPERTY,
    havingValue = "before-commit"
)
final class AcknowledgeBeforeCommit implements SmsAcknowledgementStrategy {
    private final SmsIngestionService service;
    private final TaskExecutor executor;

    AcknowledgeBeforeCommit(
        SmsIngestionService service,
        @Qualifier("earlyAcknowledgementExecutor") TaskExecutor executor
    ) {
        this.service = service;
        this.executor = executor;
    }

    @Override
    public void ingest(SmsIngestionCommand command) {
        executor.execute(() -> service.ingest(command));
    }
}
