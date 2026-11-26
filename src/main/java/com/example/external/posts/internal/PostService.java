package com.example.external.posts.internal;

import com.example.external.posts.PlatformEnum;
import com.example.external.posts.PostCreatedRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

@Service
@RequiredArgsConstructor
public class PostService {

    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public Post createPost(String author, String content, Set<PlatformEnum> platforms) {
        Post saved = Post.of(author, content, platforms);
        eventPublisher.publishEvent(
            new PostCreatedRecord(
                saved.getId(),
                saved.getAuthor(),
                saved.getContent(),
                saved.getPlatforms()
            )
        );
        return saved;
    }
}
