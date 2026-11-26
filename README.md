# external
Study for Externalize Events in Spring Modulith with Kafka

### VIDEO SOURCE: https://youtu.be/8sJIWwc7Jss?si=Ebfbf1NclNAhTSwh

### TRY
```bash
curl -X POST http://localhost:8080/api/posts \
  -H 'Content-Type: application/json' \
  -d '{
    "author": "dan",
    "content": "hello modulith",
    "platforms": ["TWITTER", "LINKEDIN", "BLUESKY"]
  }'
```
