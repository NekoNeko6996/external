package com.example.external.posts.internal;

import com.example.external.posts.PlatformEnum;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Set;
import java.util.UUID;

@Entity 
@Getter 
@NoArgsConstructor 
@AllArgsConstructor 
public class Post {
    
    @Id 
    @Column(name = "id", nullable = false)
    private UUID id;

    private String author;
    private String content;
    private Set<PlatformEnum> platforms; 

    public static Post of(String author, String content, Set<PlatformEnum> platforms) {
        return new Post(UUID.randomUUID(), author, content, platforms);
    }
}
