# external
Study for Externalize Events in Spring Modulith with Kafka

### VIDEO SOURCE: https://youtu.be/8sJIWwc7Jss?si=Ebfbf1NclNAhTSwh

# Bản minh họa tính năng Externalization (xuất sự kiện ra bên ngoài) trong Spring Modulith

Một bản minh họa tối giản về Spring Modulith: một sự kiện miền (domain event) có tên `PostCreated` được xử lý **nội bộ** bởi một mô-đun khác, đồng thời được **xuất ra bên ngoài** (externalized) tới một chủ đề (topic) trong Kafka.

## Kiến trúc

Gồm hai mô-đun và một trình lắng nghe Kafka (Kafka listener) ở cấp ứng dụng:

- **`posts`** — Điểm truy cập REST. Công khai sự kiện `PostCreated` và kiểu liệt kê (enum) `Platform`. Các thành phần triển khai (`Post`, `PostService`, `PostController`) nằm trong gói `posts.internal`.
- **`notifications`** — `NotificationService` (nằm trong `notifications.internal`) tiêu thụ sự kiện `PostCreated` thông qua chú thích `@ApplicationModuleListener` và ghi lại thông báo dành cho quản trị viên.
- **`ExternalizedApplication`** — Chứa `@KafkaListener` ở cấp ứng dụng, lắng nghe trên chủ đề `social-posts`, cho phép bạn quan sát thông điệp đã được xuất ra bên ngoài ngay trong nhật ký (logs) của ứng dụng.

`ModularityTests` sử dụng `ApplicationModules.verify()` để làm thất bại quá trình xây dựng (build) nếu có bất kỳ mô-đun nào truy cập vào gói `internal` của mô-đun khác.

## Cách chạy

```bash
./mvnw spring-boot:run
```

Tính năng hỗ trợ Docker Compose của Spring Boot sẽ tự động khởi chạy Postgres và Apache Kafka dựa trên tệp `compose.yaml`.

## Tạo một bài đăng (post)

```bash
curl -X POST http://localhost:8080/api/posts \
-H 'Content-Type: application/json' \
-d '{
"author": "dan",
"content": "hello modulith",
"platforms": ["TWITTER", "LINKEDIN", "BLUESKY"]
}'
```

Một lần xuất bản sự kiện sẽ tạo ra hai dòng nhật ký:

```
NotificationService     : Notifying admins: new post <uuid> by dan
ExternalizedApplication : Kafka [social-posts] key=<uuid> value={"id":"...","author":"dan",...}
```

Khóa (key) của thông điệp Kafka trùng khớp với ID của bài đăng nhờ vào cấu hình `@Externalized("social-posts::#{#this.id()}")`. Cả hai trình xử lý (trình lắng nghe nội bộ và cơ chế chuyển tiếp ra Kafka) đều được theo dõi trong hệ thống đăng ký sự kiện (event registry) của Modulith — câu lệnh `SELECT * FROM event_publication;` sẽ hiển thị hai hàng cho mỗi sự kiện được phát đi, với trạng thái được đánh dấu là hoàn tất (complete) khi xử lý thành công.

## Cách thức sự kiện được chuyển tiếp đến Kafka

Toàn bộ quá trình chuyển tiếp ra bên ngoài được định nghĩa chỉ bằng một chú thích (annotation) trên bản ghi sự kiện (event record):

```java
@Externalized("social-posts::#{#this.id()}")
public record PostCreated(UUID id, String author, String content, Set<Platform> platforms) {}
```

Chuỗi này tuân theo cú pháp `"target::routing-key"` của Modulith:

| Thành phần          | Ý nghĩa                                                                                                                                                                  |
|---------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `social-posts`      | **Target** (đích đến) — đối với cơ chế chuyển tiếp Kafka, đây chính là tên chủ đề (topic). |
| `::`                | Dấu phân cách giữa đích đến và khóa định tuyến (routing key). |
| `#{ ... }`          | Một biểu thức **SpEL** (Spring Expression Language) — được đánh giá tại thời điểm sự kiện được phát đi. Các dấu ngoặc báo hiệu cho Modulith rằng "nội dung bên trong là một biểu thức, không phải giá trị cố định (literal)". |
| `#this`             | Một biến SpEL do Modulith cung cấp, tham chiếu đến **thể hiện sự kiện** đang được chuyển tiếp (trong trường hợp này là bản ghi `PostCreated`). |
| `#this.id()`        | Gọi phương thức truy cập `id()` trên bản ghi — tức là lấy giá trị UUID của bài đăng. |

Đối với Kafka, **khóa định tuyến (routing key) sẽ trở thành khóa của thông điệp Kafka (message key)**. Cùng một ID bài đăng → cùng một phân vùng (partition) → đảm bảo thứ tự xử lý cho mỗi bài đăng. Nếu lược bỏ phần `::#{...}`, thông điệp sẽ không có khóa (null key); khi đó, các thông điệp sẽ được phân bổ theo kiểu xoay vòng (round-robin) giữa các phân vùng, dẫn đến việc mất đi sự đảm bảo về thứ tự xử lý. ## Hướng dẫn cấu hình — `application.properties`

```properties
spring.application.name=external

spring.docker.compose.skip.in-tests=false
spring.sql.init.mode=always

spring.jpa.hibernate.ddl-auto=update
spring.jpa.open-in-view=false

spring.modulith.events.republish-outstanding-events-on-restart=true

spring.kafka.consumer.group-id=externalized-demo
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer

logging.level.org.apache.kafka.common.config.AbstractConfig: WARN
logging.level.org.apache.kafka.common.utils.AppInfoParser: WARN
logging.level.org.apache.kafka.clients.consumer.ConsumerConfig: WARN
logging.level.org.apache.kafka.clients.producer.ProducerConfig: WARN
```

Modulith:

- **`spring.modulith.events.jdbc.schema-initialization.enabled: true`** — khi khởi động, Modulith sẽ tạo bảng `event_publication` trong Postgres. Bảng này đóng vai trò là **transactional outbox** (hộp thư đi dựa trên giao dịch) cho cả cơ chế lắng nghe nội bộ `@ApplicationModuleListener` lẫn cơ chế đẩy sự kiện ra bên ngoài qua Kafka; nhờ đó, các sự kiện chưa được gửi thành công có thể được thử lại.

Kafka consumer (điều khiển `@KafkaListener` trong `ExternalizedApplication`):

- **`group-id: externalized-demo`** — định danh nhóm consumer (consumer group) trong Kafka. Các consumer nằm trong cùng một nhóm sẽ chia sẻ các phân vùng (partition) của một topic; nếu chỉ có một mình, consumer này sẽ đọc toàn bộ dữ liệu.
- **`auto-offset-reset: earliest`** — khi...