package com.example.external.posts;


import org.jmolecules.event.annotation.Externalized;

import java.util.Set;
import java.util.UUID;

@Externalized("social-posts::#${this.id()}")
public record PostCreatedRecord(UUID id, String author, String content, Set<PlatformEnum> platforms) {

}
