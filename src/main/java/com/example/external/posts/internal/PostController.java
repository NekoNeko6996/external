package com.example.external.posts.internal;

import com.example.external.posts.PlatformEnum;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
@RequestMapping("/api/posts") 
@RequiredArgsConstructor 
public class PostController {

    private final PostService service;

    private record CreatePostRequest(String author, String content, Set<PlatformEnum> platforms) {
    }

    @PostMapping 
    @ResponseStatus(HttpStatus.CREATED)
    public Post create(@RequestBody CreatePostRequest request) {
        Post created = service.createPost(request.author(), request.content(), request.platforms());
        return created;
    }

}
