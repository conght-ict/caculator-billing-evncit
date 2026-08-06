# Quy Tắc Phát Triển & Bản Chỉ Dẫn Kỹ Thuật (Calculator Billing System)

Tài liệu này định nghĩa các quy tắc cốt lõi, quy chuẩn lập trình, và hướng dẫn kiến trúc bắt buộc áp dụng cho toàn bộ các Agent và nhà phát triển tham gia vào dự án Calculator Billing System.

---

## 1. Code Search Policy
### Mandatory
Bất kỳ khi nào cần tìm kiếm mã nguồn, định vị symbol hoặc hiểu luồng chạy của code, hãy ưu tiên sử dụng CodeGraph trước khi dùng grep hoặc tìm kiếm file thủ công.

*   **Công cụ MCP**: Sử dụng `codegraph_explore` để truy vấn trực tiếp mã nguồn của các Class, Interface, Method và theo vết các cuộc gọi (Call Graph) xuyên suốt từ mediation-service sang rating-engine và billing-worker.
*   **Dòng lệnh**: Sử dụng `codegraph explore "<symbol_name_or_query>"` trong terminal nếu công cụ MCP không khả dụng.
*   **Nguyên tắc**: Không mở hàng chục file cùng lúc để dò tìm code. Chỉ mở những file thực sự liên quan dựa trên kết quả phân tích của CodeGraph.

---

## 2. Subagent Activation Policy
Trước khi thực hiện bất kỳ hành động tìm kiếm hoặc chỉnh sửa code nào ở lượt đầu tiên, bạn phải đối chiếu yêu cầu của người dùng với danh sách các **Available skills** dưới đây và bắt buộc sử dụng công cụ `view_file` để đọc `SKILL.md` tương ứng để xưng danh định danh Subagent đó:
*   Nếu tác vụ liên quan đến **Mediation / Tiếp nhận & Kiểm tra chỉ số** (Kafka, HTTP Ingestion, Completeness, Validation Engine, mediation-service) $\rightarrow$ Bắt buộc đọc [mediation-subagent/SKILL.md](file:///e:/caculator-billing-evncit/.agents/skills/mediation-subagent/SKILL.md) đầu tiên.
*   Nếu tác vụ liên quan đến **Rating / Thuật toán Định giá & Áp biểu giá** (Netting, Stepping, TOU, rating-engine) $\rightarrow$ Bắt buộc đọc [rating-subagent/SKILL.md](file:///e:/caculator-billing-evncit/.agents/skills/rating-subagent/SKILL.md) đầu tiên.
*   Nếu tác vụ liên quan đến **Worker / Spring Batch & Kafka Worker** (Chunk-processing, Redis Cache-Aside, Bulk Write, Outbox Event, billing-worker, snapshot-generator) $\rightarrow$ Bắt buộc đọc [worker-subagent/SKILL.md](file:///e:/caculator-billing-evncit/.agents/skills/worker-subagent/SKILL.md) đầu tiên.
*   Nếu tác vụ liên quan đến **Common / Entities & DTOs dùng chung** (JPA mapping, Composite keys, JSONB mapping, Event schemas, billing-common) $\rightarrow$ Bắt buộc đọc [common-subagent/SKILL.md](file:///e:/caculator-billing-evncit/.agents/skills/common-subagent/SKILL.md) đầu tiên.

---

## 3. Java & Spring Boot Code Conventions
*   **Java Version**: Sử dụng các tính năng hiện đại của Java 21 (hoặc Java 17 tùy môi trường build thực tế) như record, text blocks, pattern matching, switch expressions một cách hợp lý.
*   **Imports**: Không import trực tiếp (fully qualified name) trong dòng code. Bắt buộc phải khai báo import ngắn gọn ở trên cùng của file và sử dụng tên class ngắn trong mã nguồn.
*   **Lombok**: Khuyến khích sử dụng `@Data`, `@Getter`, `@Setter`, `@Builder`, `@Slf4j` để giảm thiểu code boilerplate.
*   **Logging**: Luôn sử dụng `@Slf4j` với các thông tin chi tiết có cấu trúc (ví dụ: `ma_khang`, `thang_chu_ky`, `ky_chot`, `traceId`) để phục vụ tracing lỗi trên Kibana/Grafana.
*   **Exception Handling**: Tuyệt đối không được nuốt exception (empty catch). Sử dụng Custom Exception hoặc ghi rõ thông tin stack trace bằng `log.error`.
*   **Transaction**: Quản lý ranh giới giao dịch `@Transactional` chặt chẽ, đặc biệt là trong rating-engine và worker-subagent để tránh lock dữ liệu và đảm bảo rollback khi gặp lỗi batch.

---

## 4. Module & Dependency Rules
Dự án được tổ chức theo mô hình Multi-Module Maven và bắt buộc phải tuân thủ nghiêm ngặt ranh giới dependencies:
1.  **`billing-common`**: Chứa JPA Entities, DTOs, Event schemas và Helpers dùng chung. Không được import phụ thuộc ngược từ bất kỳ module nào khác.
2.  **`rating-engine`**: **Bắt buộc phải là thư viện Java thuần túy (Pure Java Library)**. Không chứa annotation của Spring (như `@Component`, `@Service`, `@Autowired`), không kết nối database, không thực hiện logging qua framework ngoài, không tự tạo thread và vô trạng thái (stateless). Việc này đảm bảo hiệu năng CPU tối đa và dễ viết Unit Test độc lập.
3.  **`mediation-service`**: Chịu trách nhiệm nạp, kiểm tra tính đầy đủ (Completeness) và hợp lệ (Validation Engine) của chỉ số. Chỉ phụ thuộc vào `billing-common`.
4.  **`snapshot-generator`**: Chịu trách nhiệm đóng băng thông tin lưới điện tĩnh thành JSONB và ghi đệm vào Redis. Chỉ phụ thuộc vào `billing-common`.
5.  **`billing-worker`**: Tiêu thụ task từ Kafka, nạp cấu hình từ Redis, gọi `rating-engine` tính toán và bulk write kết quả xuống database. Phụ thuộc vào `billing-common`, `rating-engine` và Spring Boot framework.

---

## 5. Database Operation Rules
*   **No Direct Queries in Services**: Tuyệt đối không viết câu lệnh truy vấn database (SQL, JPQL, Native Query) hoặc logic khởi tạo bảng (DDL) trực tiếp tại các lớp Service, Listener, hay Job.
*   **Repository-Only Database Access**: Tất cả các thao tác tương tác với Database phải được đóng gói và thực hiện bên trong lớp Repository.
*   **High-Performance Querying for Big Data**: Đối với các tác vụ xử lý dữ liệu lớn (Batch Insert, Batch Update, Batch Select hàng loạt), bắt buộc sử dụng **`JdbcTemplate`** hoặc **`NamedParameterJdbcTemplate`** thực hiện gom lô (batch update) và chạy raw SQL/Native Query tối ưu trên PostgreSQL để có hiệu năng cao nhất (tránh Overhead của JPA/Hibernate).
*   **JSONB Optimization**: Tận dụng kiểu dữ liệu `JSONB` của PostgreSQL để lưu trữ cấu hình biểu giá bậc thang và thông tin áp giá của điểm đo nhằm tăng tính linh hoạt và tối ưu hiệu suất truy vấn (tránh Join quá nhiều bảng).

---

## 6. Calculator Billing System Core Rules
*   **BigDecimal for Financial Data**: Tuyệt đối không sử dụng kiểu dữ liệu `double` hoặc `float` để tính toán sản lượng, đơn giá, tiền điện, thuế hoặc tổng tiền. Bắt buộc sử dụng kiểu dữ liệu **`BigDecimal`** và cấu hình rõ chế độ làm tròn **`RoundingMode.HALF_UP`** (làm tròn lên từ 5) theo chuẩn tài chính của EVN để tránh chênh lệch số lẻ.
*   **Strict Idempotency**: Mọi thao tác tính toán cước và tạo hóa đơn phải đảm bảo tính idempotent. Sử dụng khóa duy nhất `khoa_lap_trung` (`idempotency_key` kết hợp từ `ma_khang` + `thang_chu_ky` + `ky_chot` + `phien_ban_tinh`) để ngăn chặn việc tính toán hoặc phát hành hóa đơn trùng lặp khi Kafka hoặc API gửi lại thông điệp.
*   **Append-Only & Version Control**: Khi có thay đổi chỉ số hoặc điều chỉnh biểu giá, không được xóa hoặc cập nhật đè lên dữ liệu cũ. Phải tạo bản ghi mới với phiên bản tăng dần (`subReadingSeq` hoặc `phien_ban_tinh` tăng dần) và chuyển trạng thái bản ghi cũ sang `REPLACED` để đảm bảo tính lưu vết (Audit Trail).
*   **Fail-safe Batch Execution**: Trong các Job xử lý tính toán hàng loạt (Spring Batch), lỗi của một khách hàng cá biệt (nhập sai chỉ số, cấu hình giá lỗi) không được làm dừng toàn bộ tiến trình của sổ cước. Hệ thống phải bắt exception cục bộ, ghi nhận vào `nhat_ky_loi_tinh_toan`, cập nhật trạng thái khách hàng thành `FAILED` và tiếp tục xử lý các khách hàng tiếp theo.
*   **No Default Values for Missing Critical Data**: Tuyệt đối không được sử dụng giá trị mặc định (default values) khi kiểm tra và xác thực dữ liệu đầu vào. Nếu dữ liệu đầu vào hoặc cấu hình bị thiếu/không hợp lệ và có ảnh hưởng đến các luồng tính toán, hệ thống bắt buộc phải ghi nhận lỗi (ví dụ: cập nhật trạng thái sang `SUSPECT`/`PENDING_MANUAL` hoặc ném Exception thích hợp), ghi log chi tiết phục vụ đối soát và xử lý thủ công, tuyệt đối không tự động thay thế bằng giá trị mặc định khác làm sai lệch hoặc che giấu lỗi hệ thống.

---

## 7. Multi-Agent & LLM Selection Rule (Quy tắc Phối hợp Lực lượng)
Để tối ưu hóa sức mạnh của các mô hình ngôn ngữ lớn khác nhau trong quá trình pair-programming:
*   **Phân tích & Lên Kế hoạch (Planning Mode)**: Khi cần thiết kế hệ thống, phân tích nghiệp vụ phức tạp, giải quyết mâu thuẫn kiến trúc hoặc lập kế hoạch triển khai lớn $\rightarrow$ Khuyến khích người dùng chọn **Claude Opus 4.6 (Thinking)**. Sử dụng slash command `/grill-me` để phỏng vấn làm rõ các quyết định thiết kế. Kế hoạch phải được ghi nhận vào `implementation_plan.md` và được phê duyệt trước khi lập trình.
*   **Lập trình & Chạy Kiểm thử (Execution Mode)**: Khi bắt tay vào viết code, sửa lỗi compile, viết Unit Test, chạy maven test hoặc tối ưu hóa hiệu năng $\rightarrow$ Khuyến khích người dùng chọn **Gemini 3.5 Flash (High)** để có tốc độ phản hồi tức thời (<3s) và khả năng xử lý context lớn.
*   **Xác định Subagent**: Khi bắt đầu một phiên làm việc, Agent hiện tại bắt buộc phải đối chiếu Scope nghiệp vụ với Rule số 2 để đọc tệp `SKILL.md` và xướng danh Subagent tương ứng trước khi tiến hành code.
