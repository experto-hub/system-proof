package io.github.jacekkardys.systemproof.examples.ingestion;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
    name = SmsAcknowledgementStrategy.MODE_PROPERTY,
    havingValue = "before-commit"
)
class EarlyAcknowledgementConfiguration {
    @Bean(name = "earlyAcknowledgementExecutor", destroyMethod = "shutdown")
    ThreadPoolTaskExecutor earlyAcknowledgementExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("early-ack-ingestion-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }
}
