package com.example.external.notifications.internal;

import com.example.external.posts.PostCreatedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;

@Service 
public class NotificationService {

    private final Logger log = LoggerFactory.getLogger(NotificationService.class);

    @ApplicationModuleListener
    void on(PostCreatedRecord event) {
        log.info("Sending notification for {} to {}", event.id(), event.author());
    }
}
