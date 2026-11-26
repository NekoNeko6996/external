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

- **`group-id: externalized-demo`** — Định danh nhóm consumer Kafka. Các consumer trong cùng một nhóm sẽ chia sẻ các phân vùng (partition) của một topic; nếu hoạt động độc lập, một consumer sẽ đọc toàn bộ dữ liệu.
- **`auto-offset-reset: earliest`** — Khi nhóm chưa có offset nào được xác nhận (lần chạy đầu tiên hoặc sau khi thực hiện `docker compose down -v`), consumer sẽ bắt đầu đọc từ đầu topic thay vì chỉ nhận "các tin nhắn mới kể từ thời điểm hiện tại". Thiết lập này rất hữu ích cho các bản demo, giúp đảm bảo rằng ngay cả những tin nhắn được gửi trước khi consumer tham gia vẫn sẽ được hiển thị.
- **`value-deserializer: …StringDeserializer`** — Mặc định, Spring Boot sử dụng `ByteArrayDeserializer`, khiến tin nhắn nhận về dưới dạng `byte[]` và hiển thị trong log là các chuỗi số như `[123, 34, …]`. Việc chuyển sang `StringDeserializer` giúp giải mã các byte này theo chuẩn UTF-8, nhờ đó nội dung JSON trở nên dễ đọc hơn. (Nếu muốn xử lý payload theo kiểu dữ liệu cụ thể, bạn có thể sử dụng `JsonDeserializer` kèm cấu hình `trusted-packages` — tuy nhiên, cách này hơi phức tạp so với nhu cầu của bản demo này.)

Logging:

- Việc thiết lập mức log `WARN` cho bốn lớp (class) Kafka giúp loại bỏ các thông báo dài dòng như `ConsumerConfig values: { ... }` / `ProducerConfig values: { ... }` và thông tin `Kafka version: 4.1.2` thường xuất hiện mỗi khi client khởi chạy. Đây chỉ là thao tác giúp làm gọn log; bạn có thể tạm thời xóa các thiết lập này nếu cần gỡ lỗi (debug) cấu hình của client.

## Spring Initializr

Được tạo từ [start.spring.io](https://start.spring.io/#!type=maven-project&language=java&platformVersion=4.0.6&packaging=jar&configurationFileFormat=yaml&jvmVersion=26&groupId=dev.danvega&artifactId=externalized&packageName=dev.danvega.externalized&dependencies=web,modulith,kafka,data-jdbc,docker-compose,postgresql) với các tùy chọn sau:

| Thiết lập     | Giá trị                            |
|---------------|------------------------------------|
| Project       | Maven                              |
| Ngôn ngữ      | Java                               |
| Spring Boot   | 4.0.6                              |
| Java          | 26                                 |
| Đóng gói      | jar                                |
| Group         | `dev.danvega`                      |
| Artifact      | `externalized`                     |
| Định dạng cấu hình | YAML (`application.yaml`)          |

Các phụ thuộc (dependencies) và lý do sử dụng:

- **Spring Web** — cung cấp REST endpoint `/api/posts`.
- **Spring Modulith** — xác định ranh giới mô-đun (`@ApplicationModuleListener`, `ApplicationModules.verify()`) và hỗ trợ đưa sự kiện ra bên ngoài (`@Externalized`).
- **Spring for Apache Kafka** — bao gồm Kafka client và `KafkaTemplate` được cấu hình tự động (được Modulith sử dụng để phát hành sự kiện), cùng với `@KafkaListener` để tiêu thụ (consume) dữ liệu từ topic phục vụ cho bản demo này.
- **Spring Data JDBC** — tích hợp JDBC và `DataSource`; thành phần này được registry quản lý phát hành sự kiện của Modulith sử dụng để lưu trữ các sự kiện dưới dạng bảng "outbox" (bảng `event_publication`) nhằm đảm bảo tính giao dịch.
- **Docker Compose Support** — tự động khởi chạy các dịch vụ được định nghĩa trong `compose.yaml` khi chạy lệnh `./mvnw spring-boot:run`, không cần phải thực hiện thủ công lệnh `docker compose up`.
- **PostgreSQL Driver** — driver JDBC dành cho container Postgres được khởi chạy thông qua Docker Compose.

## Tài liệu

Tính năng trọng tâm của bản demo này:

- [Externalizing Application Events (Đưa các sự kiện ứng dụng ra bên ngoài)](https://docs.spring.io/spring-modulith/reference/events.html#externalization) — bao gồm `@Externalized`, cú pháp xác định đích/routing-key (`"topic::#{spel}"`) và bảng tổng hợp các broker được hỗ trợ.

Các tài liệu khác về Spring Modulith:

- [Fundamentals (Các khái niệm cơ bản)](https://docs.spring.io/spring-modulith/reference/fundamentals.html) — giải thích về API của một mô-đun, lý do tại sao các gói con (sub-package) mặc định được coi là nội bộ (internal), các giao diện được đặt tên (named interfaces), và khái niệm OPEN so với CLOSED.
- [Working with Application Events (Làm việc với các sự kiện ứng dụng)](https://docs.spring.io/spring-modulith/reference/events.html) — bao gồm `@ApplicationModuleListener`, registry quản lý phát hành sự kiện, và cơ chế đảm bảo sự kiện được gửi ít nhất một lần (at-least-once delivery). - [Xác minh cấu trúc mô-đun (Verifying Module Structure)](https://docs.spring.io/spring-modulith/reference/verification.html) — tìm hiểu những gì `ApplicationModules.verify()` kiểm tra và cách phương thức này làm gián đoạn quá trình xây dựng (build) khi phát hiện các vi phạm.

Liên quan đến Spring Boot / Kafka:

- [Gửi/nhận thông điệp với Apache Kafka (Messaging with Apache Kafka)](https://docs.spring.io/spring-boot/4.0.6/reference/messaging/kafka.html) — cơ chế tự động cấu hình (autoconfiguration) Kafka trong Spring Boot.
- [Dịch vụ phát triển với Docker Compose (Docker Compose Dev Service)](https://docs.spring.io/spring-boot/4.0.6/reference/features/dev-services.html#features.dev-services.docker-compose) — cách tệp `compose.yaml` được tích hợp vào quy trình phát triển (dev loop).