
        // Switch between main menus
        function switchMenu(menuId) {
            document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(panel => panel.classList.remove('active'));

            document.getElementById('nav-' + menuId).classList.add('active');
            document.getElementById('tab-' + menuId).classList.add('active');
        }

        // Switch between Swimlane and Technical diagram views
        function switchDiagramView(viewId) {
            document.getElementById('sub-tab-swimlane-auto').classList.remove('active');
            document.getElementById('sub-tab-swimlane-exc').classList.remove('active');
            document.getElementById('sub-tab-technical').classList.remove('active');

            document.getElementById('svg-swimlane-auto').style.display = 'none';
            document.getElementById('svg-swimlane').style.display = 'none';
            document.getElementById('svg-technical').style.display = 'none';
            document.getElementById('tech-selector-container').style.display = 'none';
            document.getElementById('exc-selector-container').style.display = 'none';

            const container = document.getElementById('diagram-layout-container');

            if (viewId === 'swimlane-auto') {
                document.getElementById('sub-tab-swimlane-auto').classList.add('active');
                document.getElementById('svg-swimlane-auto').style.display = 'block';
                if (container) container.style.gridTemplateColumns = '1fr';
                resetSwimlaneHighlights();
            } else if (viewId === 'swimlane-exc') {
                document.getElementById('sub-tab-swimlane-exc').classList.add('active');
                document.getElementById('svg-swimlane').style.display = 'block';
                document.getElementById('exc-selector-container').style.display = 'block';
                if (container) container.style.gridTemplateColumns = '1fr';
                resetExcHighlights();
            } else {
                document.getElementById('sub-tab-technical').classList.add('active');
                document.getElementById('svg-technical').style.display = 'block';
                document.getElementById('tech-selector-container').style.display = 'block';
                if (container) {
                    container.style.gridTemplateColumns = window.innerWidth > 1024 ? '2fr 1fr' : '1fr';
                }
                resetTechHighlights();
            }
        }

        // ================== DIAGRAM 1: SWIMLANE DATA ==================
        const swNodeMapping = {
            'sched': {
                title: "CMIS Lập Lịch Ghi Chỉ Số",
                tag: "CMIS Action",
                desc: "Lập lịch ghi chỉ số theo Mã Sổ (Book_ID) và Ngày chốt. Sau khi nhấn lưu lịch trên CMIS, hệ thống tự động sinh sự kiện và đồng bộ sang Billing System thông qua hàng đợi Kafka 'cmis-schedules'.",
                code: `// CMIS xuất bản event chốt cước đối tượng quản lý
{
  "bookId": "SO_Y1",
  "billingCycleMonth": "2026_06",
  "billingDate": "2026-06-15",
  "status": "ACTIVE"
}`
            },
            'billsched': {
                title: "Lưu Trữ Dữ Liệu Lịch (Billing System)",
                tag: "Database Node",
                desc: "Hệ thống chốt cước lắng nghe Kafka và lưu lịch hoạt động vào Postgres DB (bảng book_billing_schedule) để chuẩn bị chạy Job đo xa.",
                code: `@KafkaListener(topics = "cmis-schedules", groupId = "billing-group")
public void saveSchedule(CmisScheduleEvent event) {
    scheduleRepository.save(new BookBillingSchedule(event));
}`
            },
            'job': {
                title: "Job Lấy Chỉ Số Theo Lịch (Billing System)",
                tag: "Cron Job Task",
                desc: "Job lập lịch quét các Sổ đang hoạt động (ACTIVE). Định kỳ, Job sẽ gửi lệnh yêu cầu hệ thống đo xa AMR thu thập chỉ số của các khách hàng thuộc Sổ chốt cước.",
                code: `@Scheduled(cron = "0 0/15 * * * ?")
public void triggerTelemetryFetch() {
    List<BookBillingSchedule> activeBooks = scheduleRepository.findByRunStatus("ACTIVE");
    activeBooks.forEach(book -> amrClient.fetchReadings(book));
}`
            },
            'amr': {
                title: "Lấy Chỉ Số Đo Xa Thô (Billing System)",
                tag: "Integration Ingest",
                desc: "Kết nối hệ thống đo xa kéo dữ liệu chỉ số thô của từng khách hàng. Đẩy dữ liệu này qua Kafka 'meter-readings-input' về Mediation Service để đồng bộ ngược sang CMIS và lưu trữ.",
                code: `// Đẩy chỉ số thô đo xa lên Kafka
producer.send(new ProducerRecord<>("meter-readings-input", accountId, reading));`
            },
            'cmisread': {
                title: "CMIS Ghi Nhận Chỉ Số Đo Xa",
                tag: "Data Sync Target",
                desc: "CMIS tiếp nhận chỉ số đo xa thô đồng bộ ngược từ Kafka làm dữ liệu gốc của khách hàng để làm bằng chứng chốt cước.",
                code: `// CMIS SQL insert chỉ số đo xa làm cơ sở dữ liệu gốc
INSERT INTO cmis_reading_logs (account_id, period, index_value) VALUES (?, ?, ?);`
            },
            'correct': {
                title: "Hiệu Chỉnh Bổ Sung Chỉ Số",
                tag: "CMIS UI Edit",
                desc: "Với các chỉ số bị lỗi, người vận hành CMIS nhập chỉ số thực tế thay thế hoặc điều chỉnh trực tiếp trên giao diện để tính cước.",
                code: `// Sự kiện hiệu chỉnh gửi sang Billing
{
  "accountId": "KH0001",
  "correctedIndex": 1824,
  "reason": "Đo xa mất tín hiệu"
}`
            },
            'ingest': {
                title: "Billing Ghi Nhận Chỉ Số Mới",
                tag: "Kafka Consumer",
                desc: "Lắng nghe sự kiện hiệu chỉnh từ CMIS qua Kafka. Cập nhật chỉ số mới vào DB PostgreSQL và xóa cache cũ trên Redis để chuẩn bị tính cước lại.",
                code: `@KafkaListener(topics = "meter-reading-resolutions")
public void updateReading(ReadingResolutionEvent event) {
    postgresDb.updateIndex(event.getAccountId(), event.getCorrectedIndex());
    redisTemplate.delete("snapshot:" + event.getAccountId() + ":" + month);
}`
            },
            'ready': {
                title: "Kiểm Tra Sẵn Sàng (Ready? Gate 1)",
                tag: "Validation Check",
                desc: "Kiểm tra xem khách hàng đã đủ điều kiện tính cước chưa (Có đủ chỉ số đầu, chỉ số cuối, không có lỗi đo xa âm). Nếu đủ (YES) chuyển sang tính cước. Nếu thiếu chỉ số (NO), ghi log lỗi.",
                code: `boolean isReady = readings.hasStart() && readings.hasEnd() && readings.getCons() >= 0;`
            },
            'logread': {
                title: "Ghi Log Lỗi Chỉ Số",
                tag: "Database Status Update",
                desc: "Ghi nhận trạng thái PENDING hoặc FAILED vào bảng account_billing_status, log chi tiết lỗi thiếu chỉ số để đồng bộ hiển thị lên Exception Dashboard của CMIS.",
                code: `statusRepository.updateStatus(accountId, month, "PENDING");`
            },
            'chkread': {
                title: "Xác Nhận Chỉ Số (Cổng 1)",
                tag: "CMIS Gate 1",
                desc: "Bộ phận vận hành kiểm tra tình trạng chỉ số. Nhờ mô hình cuốn chiếu theo khách hàng, Cổng 1 chỉ quản lý và giữ lại các khách hàng bị lỗi chỉ số hoặc lỗi đo xa thô để hiệu chỉnh, các khách hàng sạch đã đi thẳng tới bước tiếp theo.",
                code: `// CMIS API check Cổng 1
GET /api/v1/gate1/check?bookId=SO_Y1`
            },
            'searchread': {
                title: "Tìm Kiếm Lỗi Chỉ Số",
                tag: "CMIS Search",
                desc: "Truy vấn danh sách khách hàng trong đối tượng quản lý chưa ready hoặc có lỗi đo xa để lọc ra danh sách tác nghiệp hiệu chỉnh.",
                code: `SELECT * FROM account_billing_status WHERE book_id = 'SO_Y1' AND status = 'PENDING';`
            },
            'notready': {
                title: "Quyết Định Tồn Tại Not Ready?",
                tag: "Decision logic",
                desc: "Nếu còn khách hàng thiếu chỉ số (YES) -> Bắt buộc Hiệu chỉnh/xác nhận bổ sung chỉ số. Nếu rỗng (NO) -> Chuyển sang bước Tính cước.",
                code: `if (failedList.size() > 0) { return "GOTO_CORRECTION"; } else { return "GOTO_BILLING"; }`
            },
            'resolvenotready': {
                title: "Duyệt Chỉ Số / Nhập Chỉ Số Hiệu Chỉnh",
                tag: "CMIS Operation Form",
                desc: "Nhập chỉ số hiệu chỉnh thủ công hoặc duyệt đồng ý lấy chỉ số đo xa thô cũ làm chỉ số chốt cước chính thức cho khách hàng lỗi. Tác vụ đẩy sự kiện Kafka để Billing Worker tự động tính toán lại riêng cho khách hàng này.",
                code: `POST /api/v1/readings/resolve`
            },
            'cmiscalc': {
                title: "CMIS Yêu Cầu Tính Cước",
                tag: "Action Trigger",
                desc: "Sau khi duyệt xong Cổng 1 chỉ số, CMIS phát lệnh tính cước hàng loạt qua Kafka topic 'cmis-batch-requests'.",
                code: `{
  "operation": "RUN_CALCULATION",
  "bookId": "SO_Y1"
}`
            },
            'billcalc': {
                title: "Tính Hóa Đơn (RAM Engine)",
                tag: "Billing Worker",
                desc: "Billing Worker nạp cấu hình hợp đồng từ Redis, thực hiện áp giá bậc thang sinh hoạt hoặc giá sản xuất netting trên bộ nhớ RAM nhanh chóng (<10ms) và ghi hóa đơn nháp xuống Postgres.",
                code: `CalculationResult result = ratingEngine.calculate(snapshot, readings);
invoiceRepository.save(result.toDraftInvoice());`
            },
            'anomaly': {
                title: "Đánh Giá Bất Thường (Gate 2)",
                tag: "Validation Check",
                desc: "Worker đánh giá hóa đơn nháp. Nếu sản lượng vọt x2 hoặc tổng số tiền hóa đơn trước thuế > 1,000,000 VND, hóa đơn tự động chuyển trạng thái ANOMALY. Nếu bình thường chuyển trạng thái SUCCESS.",
                code: `if (invoice.getTotalBeforeTax() > 1000000.0) {
    status.setStatus("ANOMALY");
} else {
    status.setStatus("SUCCESS");
}`
            },
            'searchanomaly': {
                title: "Tìm Kiếm Hóa Đơn Bất Thường",
                tag: "CMIS UI Search",
                desc: "Truy vấn danh sách hóa đơn bị đánh dấu ANOMALY để hiển thị lên cổng phê duyệt bất thường.",
                code: `SELECT * FROM account_billing_status WHERE status = 'ANOMALY' AND book_id = 'SO_Y1';`
            },
            'saveinvoice': {
                title: "Lưu Hóa Đơn Nháp / Gốc",
                tag: "Database Storage",
                desc: "Ghi nhận hóa đơn nháp hoặc hóa đơn chính thức của khách hàng làm căn cứ kiểm duyệt và đối soát tài chính.",
                code: `INSERT INTO bill_invoice (account_id, billing_month, amount) VALUES (?, ?, ?);`
            },
            'loganomaly': {
                title: "Ghi Nhận Log Bất Thường",
                tag: "DB Log Update",
                desc: "Cập nhật trạng thái ANOMALY của khách hàng để nhân viên hậu kiểm biết lý do hóa đơn bị chặn.",
                code: `statusRepository.updateStatus(accountId, month, "ANOMALY");`
            },
            'cmanomaly': {
                title: "Quyết Định Tồn Tại Bất Thường?",
                tag: "Decision logic",
                desc: "Nếu còn khách hàng ở trạng thái ANOMALY (YES) -> Bắt buộc rà soát phê duyệt/Hủy tính. Nếu rỗng (NO) -> Chuyển sang Cổng 3 Ký HĐĐT.",
                code: `if (anomalyCount > 0) { return "GOTO_APPROVAL"; } else { return "GOTO_INVOICING"; }`
            },
            'resolveanomaly': {
                title: "Duyệt Bất Thường / Hủy Tính (Cổng 2)",
                tag: "CMIS Gate 2 Action",
                desc: "Nhân viên phê duyệt hóa đơn bất thường. Nếu Duyệt bất thường: bắn sự kiện sang Billing chuyển trạng thái hóa đơn sang 'Sẵn sàng lập HĐĐT' (READY_FOR_E_INVOICE). Nếu Hủy tính: bắn sự kiện sang Billing chuyển trạng thái hóa đơn sang 'Hủy tính' (CANCELLED_BILLING) để quay lại Cổng 1 hiệu chỉnh chỉ số.",
                code: `// Sự kiện gửi qua Kafka billing-operations-topic
// 1. Duyệt bất thường (Sẵn sàng lập HĐĐT):
{
  "accountId": "KH0002",
  "status": "READY_FOR_E_INVOICE"
}
// 2. Hủy tính cước (Hủy tính):
{
  "accountId": "KH0002",
  "status": "CANCELLED_BILLING"
}`
            },
            'waiting': {
                title: "Hàng Đợi Chờ Tính Hóa Đơn (Cổng 1.5)",
                tag: "CMIS Waiting Queue",
                desc: "Hàng đợi lưu trữ tạm thời các khách hàng đã sẵn sàng tính cước (chỉ số ready sau khi hiệu chỉnh hoặc đo xa sạch) hoặc các khách hàng bị chủ động hủy tính từ Cổng 2. Tại đây, bộ phận vận hành có thể yêu cầu tính toán cước lại hoặc trả ngược về Cổng 1 để sửa lại chỉ số.",
                code: `// Trạng thái lưu trữ trong Redis hoặc DB
statusRepository.updateStatus(accountId, month, "WAITING_CALCULATION");`
            },
            'issue': {
                title: "Đồng Bộ Trạng Thái Chốt Cước (Cổng 3)",
                tag: "CMIS Gate 3 Action",
                desc: "Lập hóa đơn điện tử cho các khách hàng đạt SUCCESS trên CMIS. Đồng bộ trạng thái chốt cước (khóa cước) sang Billing System qua REST API.",
                code: `// Gọi REST API đồng bộ trạng thái chốt cước
POST /api/v1/monitoring/billing/operation?operationType=LOCK_ACCOUNTS&accountId=KH0001`
            },
            'saveeinvoice': {
                title: "Lưu Hóa Đơn Điện Tử & Khóa Cứng (CMIS)",
                tag: "Finalize & Block",
                desc: "CMIS thực hiện lưu trữ hóa đơn điện tử chính thức và khóa cứng trạng thái (LOCKED) trên hệ thống để kết thúc chu trình chốt cước.",
                code: `// Khóa trạng thái chốt cước trên Redis
redisTemplate.opsForHash().put("billing:book_status_hash:SO_Y1", accountId, "LOCKED");`
            }
        };

        function showSwNode(nodeKey) {
            const data = swNodeMapping[nodeKey];
            if (!data) return;

            // Highlight node
            document.querySelectorAll('.node-group').forEach(node => {
                node.classList.remove('active-node');
            });
            const targetNode = document.getElementById('node-sw-' + nodeKey);
            if (targetNode) {
                targetNode.classList.add('active-node');
            }
            const targetNodeAuto = document.getElementById('node-sw-' + nodeKey + '-auto');
            if (targetNodeAuto) {
                targetNodeAuto.classList.add('active-node');
            }

            // Fill details panel
            document.getElementById('detail-title').innerText = data.title;
            document.getElementById('detail-tag').innerText = data.tag;
            document.getElementById('detail-tag').className = 'badge badge-purple';
            document.getElementById('detail-desc').innerText = data.desc;

            document.getElementById('detail-code-wrapper').style.display = 'block';
            document.getElementById('detail-code').innerText = data.code;
        }

        function resetSwimlaneHighlights() {
            document.querySelectorAll('.node-group').forEach(node => node.classList.remove('active-node'));
            document.getElementById('detail-title').innerText = "Chi tiết luồng xử lý";
            document.getElementById('detail-desc').innerText = "Nhấn vào các khối hoặc nút quy trình trên sơ đồ Swimlane để xem thông tin nghiệp vụ chi tiết, các tham số trao đổi và mã nguồn minh họa.";
            document.getElementById('detail-code-wrapper').style.display = 'none';
        }


        // ================== DIAGRAM 2: TECHNICAL DETAILS ==================
        const techNodeMapping = {
            'cmis': {
                title: "CMIS Portal (Hệ thống Nguồn gốc & Hóa đơn)",
                tag: "Core Legacy System",
                desc: "Hệ thống quản lý thông tin khách hàng gốc của EVN. Nơi lưu trữ thông tin chỉ số công tơ, định mức giá và lịch sử cước của từng khách hàng. Khi cần chốt cước hoặc hủy cước cho khách hàng, CMIS sẽ gửi thông điệp tương ứng qua Kafka.",
                code: `// Yêu cầu xử lý gửi từ CMIS
POST /api/v1/monitoring/billing/operation?accountId=KH0001`
            },
            'kafka': {
                title: "Kafka Message Broker (Event Hub)",
                tag: "Event Broker Cluster",
                desc: "Hệ thống hàng đợi tin nhắn trung chuyển bất đồng bộ kết nối CMIS Portal và Billing System. Chứa toàn bộ các topic: meter-reading-resolutions (chỉ số Cổng 1), cmis-schedules (lập lịch), billing-cancellation-topic (hủy cước Cổng 2), và invoice-outbound (đồng bộ hóa đơn ngược về CMIS). Hỗ trợ truyền thông điệp cuốn chiếu theo khách hàng.",
                code: `Topics:
1. meter-reading-resolutions
2. cmis-schedules
3. billing-cancellation-topic
4. invoice-outbound`
            },
            'mediation': {
                title: "Mediation Service (Cổng giao tiếp)",
                tag: "Spring Boot Microservice",
                desc: "Cổng tiếp nhận và chuẩn hóa dữ liệu. Khi nhận thông điệp chỉ số hoặc yêu cầu từ Kafka, Mediation Service phân tích, đối chiếu tính sẵn sàng của dữ liệu cho khách hàng đó và gửi lệnh tính toán vào hàng đợi xử lý.",
                code: `@KafkaListener(topics = "meter-reading-resolutions")
public void onReading(CmisReadingEvent event) { ... }`
            },
            'orchestrator': {
                title: "Batch Orchestrator",
                tag: "Spring Batch Master",
                desc: "Bộ điều phối tiến trình tính cước của khách hàng. Nó nạp cấu hình tính toán (biểu giá, định mức bậc thang) từ cơ sở dữ liệu lên Redis cache làm snapshot và kích hoạt luồng tính cước riêng cho khách hàng đó.",
                code: `@Bean
public Job billingJob() { ... }`
            },
            'redis': {
                title: "Redis Cluster Cache",
                tag: "Distributed In-Memory DB",
                desc: "Bộ nhớ đệm RAM lưu trữ Snapshot thông tin hợp đồng, công tơ và biểu giá áp dụng của khách hàng. Giúp Billing Worker truy xuất và tính toán cước tức thời mà không cần truy vấn đĩa cứng cơ sở dữ liệu.",
                code: `Key schema: snapshot:{accountId}:{month}`
            },
            'postgres': {
                title: "PostgreSQL Database",
                tag: "RDBMS Storage",
                desc: "Cơ sở dữ liệu lưu trữ kết quả tính cước chính thức của khách hàng (lượng điện tiêu thụ, số tiền theo từng bậc thang, thuế VAT) và nhật ký trạng thái xử lý.",
                code: `Table: bill_invoice, account_billing_status`
            },
            'cdc': {
                title: "Debezium CDC Engine",
                tag: "Change Data Capture",
                desc: "Bộ quét thay đổi cơ sở dữ liệu (Change Data Capture). Lắng nghe sự kiện ghi nhận hóa đơn mới hoặc hủy cước của khách hàng từ bảng Outbox để lập tức bắn message đồng bộ kết quả ngược lại cho CMIS.",
                code: `Connector: PostgreSQL Outbox Connector`
            },
            'kafka-execution': {
                title: "Kafka Topic: billing-execution-topic",
                tag: "High-Throughput Queue",
                desc: "Hàng đợi chứa các tác vụ tính cước chờ xử lý. Mỗi message tương ứng với một yêu cầu tính cước của một khách hàng cụ thể.",
                code: `Topic: billing-execution-topic`
            },
            'worker': {
                title: "Billing Worker Node",
                tag: "Virtual Threads Computational Engine",
                desc: "Công cụ tính cước sử dụng Virtual Threads (Java 21). Thực hiện tính tiền điện bậc thang của khách hàng trên bộ nhớ RAM dựa vào chỉ số tiêu thụ và cấu hình biểu giá từ Redis, sau đó tự động phân loại trạng thái cước (Thành công/Bất thường).",
                code: `ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();`
            }
        };

        const techWorkflowLines = {
            1: [ // Luồng 1: Tính cuốn chiếu
                { id: 'line1-1', mode: 'write' },
                { id: 'line1-2', mode: 'read' },
                { id: 'line1-3', mode: 'read' },
                { id: 'line1-4', mode: 'write' },
                { id: 'line1-5', mode: 'write' },
                { id: 'line1-6', mode: 'read' },
                { id: 'line1-7', mode: 'read' },
                { id: 'line1-8', mode: 'write' },
                { id: 'line1-9', mode: 'read' },
                { id: 'line1-10', mode: 'write' }
            ],
            2: [ // Luồng 2: Master data
                { id: 'line2-1', mode: 'write' },
                { id: 'line2-2', mode: 'read' },
                { id: 'line2-3', mode: 'write' }
            ],
            3: [ // Luồng 3: Hủy cước (bắn tin nhắn từ CMIS qua Kafka)
                { id: 'line3-1', mode: 'write' },
                { id: 'line3-2', mode: 'read' },
                { id: 'line3-3', mode: 'write' },
                { id: 'line1-8', mode: 'write' },
                { id: 'line1-9', mode: 'read' },
                { id: 'line1-10', mode: 'write' }
            ]
        };

        function showTechNode(nodeKey) {
            const data = techNodeMapping[nodeKey];
            if (!data) return;

            // Highlight node
            document.querySelectorAll('#svg-technical .node-group').forEach(node => {
                node.classList.remove('active-node');
            });
            const targetNode = document.getElementById('node-' + nodeKey);
            if (targetNode) {
                targetNode.classList.add('active-node');
            }

            // Fill details panel
            document.getElementById('detail-title').innerText = data.title;
            document.getElementById('detail-tag').innerText = data.tag;
            document.getElementById('detail-tag').className = 'badge badge-purple';
            document.getElementById('detail-desc').innerText = data.desc;

            document.getElementById('detail-code-wrapper').style.display = 'block';
            document.getElementById('detail-code').innerText = data.code;
        }

        const techWorkflowNodes = {
            1: ['cmis', 'kafka', 'mediation', 'redis', 'kafka-execution', 'worker', 'postgres', 'cdc'],
            2: ['cmis', 'kafka', 'mediation', 'redis'],
            3: ['cmis', 'kafka', 'orchestrator', 'postgres', 'cdc']
        };

        function highlightTechWorkflow(wfId) {
            resetTechHighlights();

            // Hide (fade out) all flow lines first
            document.querySelectorAll('#svg-technical .flow-line').forEach(line => {
                line.style.opacity = '0.05';
            });

            // Hide (fade out) all node groups first
            document.querySelectorAll('#svg-technical .node-group').forEach(node => {
                node.style.opacity = '0.15';
            });

            // Highlight specific active lines
            const lines = techWorkflowLines[wfId];
            if (!lines) return;

            lines.forEach(lineObj => {
                const el = document.getElementById(lineObj.id);
                if (el) {
                    el.classList.add('active-' + lineObj.mode);
                    el.setAttribute('marker-end', 'url(#arrow-t-' + lineObj.mode + ')');
                    el.style.opacity = '1';
                }
            });

            // Highlight specific active nodes
            const activeNodeKeys = techWorkflowNodes[wfId];
            if (activeNodeKeys) {
                activeNodeKeys.forEach(key => {
                    const el = document.getElementById('node-' + key);
                    if (el) {
                        el.style.opacity = '1';
                    }
                });
            }

            // Update details to workflow details
            const descriptions = {
                1: "Luồng 1: Tính Cước Cuốn Chiếu - Khi nhận chỉ số đo xa của khách hàng qua Kafka, Billing System xác minh tính sẵn sàng, tính tiền điện bậc thang trên RAM và lưu hóa đơn, sau đó CDC đẩy kết quả về CMIS.",
                2: "Luồng 2: Đồng bộ dữ liệu tĩnh (Master Data) - Lắng nghe các thay đổi về hợp đồng, định mức và biểu giá của khách hàng từ CMIS truyền qua Kafka để cập nhật tức thời vào Redis cache.",
                3: "Luồng 3: Hủy cước & Hồi trả (Reversion) - CMIS bắn tin nhắn yêu cầu hủy cước của khách hàng qua Kafka, Orchestrator thu hồi cước trên DB Postgres, giải phóng khóa và CDC đồng bộ ngược trạng thái về CMIS."
            };
            document.getElementById('detail-title').innerText = "Workflow Luồng " + wfId;
            document.getElementById('detail-tag').innerText = "Active Workflow";
            document.getElementById('detail-tag').className = "badge badge-success";
            document.getElementById('detail-desc').innerText = descriptions[wfId];
            document.getElementById('detail-code-wrapper').style.display = 'none';
        }

        function resetTechHighlights() {
            document.querySelectorAll('#svg-technical .flow-line').forEach(line => {
                line.classList.remove('active-write', 'active-read', 'active-trigger');
                line.setAttribute('marker-end', 'url(#arrow-t)');
                line.style.opacity = '1'; // restore full opacity
            });
            document.querySelectorAll('#svg-technical .node-group').forEach(node => {
                node.classList.remove('active-node');
                node.style.opacity = '1'; // restore full opacity
            });
            document.getElementById('detail-title').innerText = "Chi tiết luồng kỹ thuật";
            document.getElementById('detail-desc').innerText = "Chọn một luồng hệ thống ở bên dưới hoặc nhấn trực tiếp vào các nút thiết bị để xem mô tả cấu trúc mã nguồn chi tiết.";
            document.getElementById('detail-code-wrapper').style.display = 'none';
        }

        const excWorkflowPaths = {
            1: ['1', '2', '3', '4', '4b', '5'],
            2: ['1', '2', '6', '6b', '7', '8', '9', '10', '11', '12', '13'],
            3: ['1', '2', '6', '6b', '7', '8b', '9', '4c', '4', '4b', '5'],
            4: ['1', '2', '6', '6b', '10', '11', '12', '13']
        };
        const excWorkflowNodes = {
            1: ['sched', 'ready', 'resolvenotready', 'waiting', 'amr', 'billcalc'],
            2: ['sched', 'ready', 'waiting', 'anomaly', 'resolveanomaly', 'loganomaly', 'notready', 'issue', 'lock', 'saveeinvoice', 'amr', 'billcalc'],
            3: ['sched', 'ready', 'waiting', 'anomaly', 'resolveanomaly', 'loganomaly', 'resolvenotready', 'amr', 'billcalc'],
            4: ['sched', 'ready', 'waiting', 'anomaly', 'notready', 'issue', 'lock', 'saveeinvoice', 'amr', 'billcalc']
        };

        function highlightExcWorkflow(caseId) {
            resetExcHighlights();

            // Hide (fade out) all elements in svg-swimlane first
            document.querySelectorAll('#svg-swimlane .sw-line').forEach(line => {
                line.style.opacity = '0.05';
            });
            document.querySelectorAll('#svg-swimlane > text').forEach(text => {
                const textAnchor = text.getAttribute('x');
                // Keep lane labels visible
                if (textAnchor !== "300" && textAnchor !== "900") {
                    text.style.opacity = '0.15';
                }
            });
            document.querySelectorAll('#svg-swimlane .node-group').forEach(node => {
                node.style.opacity = '0.15';
            });

            // Highlight active paths and nodes
            const activePaths = excWorkflowPaths[caseId];
            if (activePaths) {
                activePaths.forEach(suffix => {
                    const lineEl = document.getElementById('sw2-path-' + suffix);
                    if (lineEl) lineEl.style.opacity = '1';
                    const textEl = document.getElementById('sw2-text-' + suffix);
                    if (textEl) textEl.style.opacity = '1';
                });
            }

            const activeNodes = excWorkflowNodes[caseId];
            if (activeNodes) {
                activeNodes.forEach(key => {
                    const el = document.getElementById('node-sw-' + key + '-sw2') || document.getElementById('node-sw-' + key);
                    if (el) el.style.opacity = '1';
                });
            }

            // Dynamically adjust Path 4b label based on Case category (automatic vs active cancel recalculate)
            const label4b = document.getElementById('sw2-text-4b');
            if (label4b) {
                if (caseId === 3) {
                    label4b.textContent = "Yêu cầu tính cước lại (CMIS Event)";
                } else {
                    label4b.textContent = "Tự động tạo event tính hóa đơn";
                }
            }

            const descriptions = {
                1: "Trường hợp 1: Chỉ số chưa ready - Chỉ số đo xa hoặc thủ công bị lỗi, rà soát hiệu chỉnh tại Cổng 1 rồi đẩy vào Hàng đợi chờ tính để người dùng gửi lệnh tính lại.",
                2: "Trường hợp 2: Hóa đơn bất thường, có duyệt tính - Phát hiện sản lượng vọt x2, duyệt bất thường tại Cổng 2, bắn sự kiện chuyển trạng thái sang Sẵn sàng lập HĐĐT, rồi tiến hành lập HĐĐT ở Cổng 3.",
                3: "Trường hợp 3: Hóa đơn bất thường, hủy tính - Phát hiện bất thường nhưng không phê duyệt, người dùng chọn Hủy tính cước, đưa vào Hàng đợi chờ tính, chuyển trả về Cổng 1 hiệu chỉnh chỉ số rồi chạy lại luồng tính.",
                4: "Trường hợp 4 (Happy Case): Chỉ số ready, hóa đơn bình thường - Chỉ số sạch đi thẳng vào Hàng đợi chờ tính, tính toán tự động thành công và chuyển sang Cổng 3 lập HĐĐT không phát sinh bất thường."
            };
            document.getElementById('detail-title').innerText = "Trường hợp xử lý: Case " + caseId;
            document.getElementById('detail-tag').innerText = "Active Scenario";
            document.getElementById('detail-tag').className = "badge badge-info";
            document.getElementById('detail-desc').innerText = descriptions[caseId];
            document.getElementById('detail-code-wrapper').style.display = 'none';
        }

        function resetExcHighlights() {
            document.querySelectorAll('#svg-swimlane .sw-line').forEach(line => {
                line.style.opacity = '1';
            });
            document.querySelectorAll('#svg-swimlane text').forEach(text => {
                text.style.opacity = '1';
            });
            document.querySelectorAll('#svg-swimlane .node-group').forEach(node => {
                node.style.opacity = '1';
            });

            const label4b = document.getElementById('sw2-text-4b');
            if (label4b) {
                label4b.textContent = "Tự động tạo event / Yêu cầu tính cước lại";
            }

            document.getElementById('detail-title').innerText = "Chi tiết luồng xử lý ngoại lệ";
            document.getElementById('detail-tag').innerText = "Ready";
            document.getElementById('detail-tag').className = "badge badge-purple";
            document.getElementById('detail-desc').innerText = "Chọn một trong các trường hợp xử lý ở trên hoặc nhấn vào các khối trên sơ đồ để xem chi tiết.";
            document.getElementById('detail-code-wrapper').style.display = 'none';
        }




        // ================== MENU 2: LIVE SIMULATOR DATA ==================
        // Simulation database store
        let simData = {
            "SO_Y1": {
                name: "đối tượng quản lý TBA Yên Hòa 1",
                kh: 50,
                readingsError: 2,      // Số lượng nhận đo xa lỗi
                indexError: 3,         // Số lượng xác nhận chỉ số lỗi
                waitingCalc: 4,        // Số lượng chờ tính hóa đơn
                anomalies: 1,          // Số lượng hóa đơn bất thường
                success: 40,           // Số lượng hóa đơn tính thành công
                status: "READY",
                scheduled: true
            },
            "SO_DV2": {
                name: "đối tượng quản lý TBA Dịch Vọng 2",
                kh: 50,
                readingsError: 0,
                indexError: 0,
                waitingCalc: 0,
                anomalies: 0,
                success: 50,
                status: "SUCCESS",
                scheduled: true
            },
            "SO_CG3": {
                name: "đối tượng quản lý TBA Cầu Giấy 3",
                kh: 50,
                readingsError: 1,
                indexError: 2,
                waitingCalc: 10,
                anomalies: 0,
                success: 37,
                status: "READY",
                scheduled: true
            }
        };

        // Render table
        function renderSimTable() {
            const tbody = document.getElementById('sim-table-body');
            tbody.innerHTML = '';

            let count = 0;
            for (const bookId in simData) {
                const book = simData[bookId];
                count++;
                const tr = document.createElement('tr');

                // Badges class matching value
                const readingsBadge = book.readingsError > 0 ? `<span class="badge badge-danger" style="cursor:pointer;" onclick="openGateModal('${bookId}', 'readings')">${book.readingsError} KH</span>` : `<span class="badge badge-success">0 KH</span>`;
                const indexBadge = book.indexError > 0 ? `<span class="badge badge-warning" style="cursor:pointer;" onclick="openGateModal('${bookId}', 'index')">${book.indexError} KH</span>` : `<span class="badge badge-success">0 KH</span>`;
                const waitingBadge = book.waitingCalc > 0 ? `<span class="badge badge-purple" style="background:rgba(139,92,246,0.2); cursor:pointer;" onclick="openGateModal('${bookId}', 'waiting')">${book.waitingCalc} KH</span>` : `<span class="badge badge-success">0 KH</span>`;
                const anomalyBadge = book.anomalies > 0 ? `<span class="badge badge-warning" style="background:rgba(245,158,11,0.2); cursor:pointer;" onclick="openGateModal('${bookId}', 'anomaly')">${book.anomalies} KH</span>` : `<span class="badge badge-success">0 KH</span>`;
                const successBadge = `<span class="badge badge-success" style="cursor:pointer;" onclick="openGateModal('${bookId}', 'success')">${book.success} KH</span>`;

                let statusBadge = '';
                if (book.status === 'READY') {
                    statusBadge = `<span class="badge badge-purple">SẮN SÀNG</span>`;
                } else if (book.status === 'SUCCESS') {
                    statusBadge = `<span class="badge badge-success">HOÀN TẤT</span>`;
                } else if (book.status === 'LOCKED') {
                    statusBadge = `<span class="badge badge-purple" style="background:#3F51B5; color:white;">ĐÃ KHÓA (LOCKED)</span>`;
                } else {
                    statusBadge = `<span class="badge badge-warning">ĐANG TÍNH</span>`;
                }

                tr.innerHTML = `
                    <td><input type="checkbox" style="accent-color:#3F51B5;"></td>
                    <td><strong>${bookId}</strong></td>
                    <td>${book.name}</td>
                    <td style="text-align: center;">${readingsBadge}</td>
                    <td style="text-align: center;">${indexBadge}</td>
                    <td style="text-align: center;">${waitingBadge}</td>
                    <td style="text-align: center;">${anomalyBadge}</td>
                    <td style="text-align: center;">${successBadge}</td>
                    <td>${statusBadge}</td>
                `;
                tbody.appendChild(tr);
            }
            const countEl = document.getElementById('sim-table-count');
            if (countEl) countEl.innerText = "Tổng số: " + count + " bản ghi";
        }

        function updateSimForm() {
            const bookId = document.getElementById('sim-book-id').value;
            const displayEl = document.getElementById('selected-gcs-display');
            if (displayEl) {
                displayEl.innerText = bookId;
            }
        }

        // Add line to Console Log Stream
        function consoleLog(topic, eventName, payload) {
            const consoleEl = document.getElementById('console-stream');
            const now = new Date();
            const timeStr = now.toTimeString().split(' ')[0];

            const div = document.createElement('div');
            div.className = 'console-line';
            div.innerHTML = `
                <span class="timestamp">[${timeStr}]</span>
                <span class="topic">${topic}</span>
                <span class="event-name">${eventName}</span>
                <pre>${JSON.stringify(payload, null, 2)}</pre>
            `;
            consoleEl.appendChild(div);
            consoleEl.scrollTop = consoleEl.scrollHeight;
        }

        // Action: Lập lịch
        function triggerScheduleSimulation() {
            const bookId = document.getElementById('sim-book-id').value;
            const date = document.getElementById('sim-sched-date').value;
            const book = simData[bookId];

            book.scheduled = true;

            // 1. Log Kafka cmis-schedules
            const payload = {
                "bookId": bookId,
                "billingCycleMonth": date.substring(0, 7).replace('-', '_'),
                "billingDate": date,
                "status": "ACTIVE"
            };
            consoleLog("cmis-schedules", "SCHEDULE_CREATED", payload);

            // 2. Simulating system reaction
            setTimeout(() => {
                consoleLog("meter-readings-input", "TELEMETRY_INGESTION_START", {
                    "bookId": bookId,
                    "targetAccountsCount": book.kh,
                    "triggerType": "AUTO_AMR_JOB"
                });
            }, 1000);

            alert("Đã gửi lịch chốt cước đối tượng quản lý " + book.name + " sang Billing System qua Kafka!");
            renderSchedTable();
        }

        // Gated Operations details mapping for CMIS Panel (Menu 2 Right)
        let activeOp = { bookId: '', type: '' };

        function openGateModal(bookId, type) {
            activeOp = { bookId, type };
            const book = simData[bookId];

            const modal = document.getElementById('operation-modal');
            if (modal) modal.classList.add('active');

            const titleEl = document.getElementById('op-title');
            const contentEl = document.getElementById('op-content');
            const controlsEl = document.getElementById('op-form-controls');

            controlsEl.innerHTML = '';

            if (type === 'readings') {
                titleEl.innerText = "🛠️ Xử lý lỗi đo xa thô (Cổng 1 - Ingestion Error)";
                contentEl.innerHTML = `
                    đối tượng quản lý chốt cước đang có <strong>${book.readingsError} khách hàng</strong> bị mất tín hiệu đo xa (AMR Ingestion Failures).<br>
                    <strong>Tác vụ:</strong> Duyệt chỉ số thô mặc định hoặc nhập lại chỉ số để đẩy lên Kafka sửa lỗi.
                `;
                controlsEl.innerHTML = `
                    <div class="form-group">
                        <label>Mã khách hàng lỗi:</label>
                        <select id="op-acc-id" class="form-control">
                            <option value="KH0012">KH0012 (Nguyễn Văn A) - CS thô: rỗng</option>
                            <option value="KH0034">KH0034 (Lê Văn B) - CS thô: -9999 (âm)</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Chọn cách xử lý:</label>
                        <select id="op-resolve-action" class="form-control" onchange="toggleOpValueInput()">
                            <option value="USE_FALLBACK">Dùng chỉ số cũ chốt cước</option>
                            <option value="MANUAL_INPUT">Nhập chỉ số hiệu chỉnh thủ công</option>
                        </select>
                    </div>
                    <div class="form-group" id="op-value-group" style="display:none;">
                        <label>Nhập chỉ số mới:</label>
                        <input type="number" id="op-corrected-val" class="form-control" placeholder="Ví dụ: 1250">
                    </div>
                `;
            } else if (type === 'index') {
                titleEl.innerText = "🔎 Xác nhận & Hiệu chỉnh chỉ số (Cổng 1 - Index Verification)";
                contentEl.innerHTML = `
                    Hệ thống ghi nhận <strong>${book.indexError} khách hàng</strong> bị cảnh báo nghi ngờ hoặc sai lệch chỉ số (sản lượng vọt x3).<br>
                    <strong>Tác vụ:</strong> Xác nhận chỉ số đấy là đúng hoặc hiệu chỉnh lại chỉ số mới để tool Billing chốt lại cuốn chiếu.
                `;
                controlsEl.innerHTML = `
                    <div class="form-group">
                        <label>Mã khách hàng nghi ngờ:</label>
                        <select id="op-acc-id" class="form-control">
                            <option value="KH0015">KH0015 (Trần Thị C) - Sản lượng vọt 450kWh (x3 trung bình)</option>
                            <option value="KH0018">KH0018 (Phạm Văn D) - Công tơ phụ nhảy ngược</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Tác vụ kiểm duyệt:</label>
                        <select id="op-resolve-action" class="form-control" onchange="toggleOpValueInput()">
                            <option value="ACCEPT_AS_IS">Xác nhận chỉ số đo xa ĐÚNG (Duyệt tính)</option>
                            <option value="CORRECT">Nhập chỉ số hiệu chỉnh mới</option>
                        </select>
                    </div>
                    <div class="form-group" id="op-value-group" style="display:none;">
                        <label>Nhập chỉ số mới hiệu chỉnh:</label>
                        <input type="number" id="op-corrected-val" class="form-control" placeholder="Nhập chỉ số thực tế">
                    </div>
                `;
            } else if (type === 'anomaly') {
                titleEl.innerText = "⚠️ Phê duyệt bất thường hóa đơn (Cổng 2 - Anomaly Gate)";
                contentEl.innerHTML = `
                    Phát hiện <strong>${book.anomalies} khách hàng</strong> có số tiền điện hóa đơn trước thuế vượt quá hạn mức 1,000,000đ.<br>
                    <strong>Tác vụ:</strong> Duyệt xác nhận hóa đơn bất thường này OK hoặc Hủy tính cước để hiệu chỉnh.
                `;
                controlsEl.innerHTML = `
                    <div class="form-group">
                        <label>Mã khách hàng bất thường:</label>
                        <select id="op-acc-id" class="form-control">
                            <option value="KH0088">KH0088 (Công ty X) - HĐ nháp: 2,845,000đ trước thuế</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Hành động duyệt:</label>
                        <select id="op-resolve-action" class="form-control">
                            <option value="LOCK_CMIS">Xác nhận bất thường OK (Duyệt tính)</option>
                            <option value="CANCEL_BILLING">Hủy tính cước (Chuyển sang hàng đợi chờ tính)</option>
                        </select>
                    </div>
                `;
            } else if (type === 'waiting') {
                titleEl.innerText = "⚙️ Xử lý nhóm chờ tính hóa đơn (Waiting Queue)";
                contentEl.innerHTML = `
                    đối tượng quản lý đang có <strong>${book.waitingCalc} khách hàng</strong> đang chờ tính hóa đơn (gồm khách hàng có chỉ số ready và khách hàng bị hủy tính chủ động từ Cổng 2).<br>
                    <strong>Tác vụ:</strong> Yêu cầu chạy tính toán cước lại hoặc chuyển trả về Cổng 1 để hiệu chỉnh lại chỉ số.
                `;
                controlsEl.innerHTML = `
                    <div class="form-group">
                        <label>Chọn khách hàng trong hàng đợi:</label>
                        <select id="op-acc-id" class="form-control">
                            <option value="KH0010">KH0010 (Nguyễn Văn E) - Chỉ số ready</option>
                            <option value="KH0022">KH0022 (Trần Văn F) - Hủy tính chủ động</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Chọn hành động:</label>
                        <select id="op-resolve-action" class="form-control">
                            <option value="RECALCULATE">⚡ Yêu cầu tính cước (Chạy Luồng Tính)</option>
                            <option value="RETURN_TO_GATE1">↩️ Quay về hiệu chỉnh chỉ số (Cổng 1)</option>
                        </select>
                    </div>
                `;
            } else if (type === 'success') {
                titleEl.innerText = "✍️ Đồng Bộ Trạng Thái Chốt Cước (Cổng 3)";
                contentEl.innerHTML = `
                    đối tượng quản lý biến áp đang có <strong>${book.success} khách hàng</strong> đã tính cước thành công và sẵn sàng phát hành hóa đơn tài chính.<br>
                    <strong>Tác vụ:</strong> Đồng bộ trạng thái chốt cước (LOCKED) sang Billing System, CMIS thực hiện lưu trữ HĐĐT và khóa sổ.
                `;
                controlsEl.innerHTML = `
                    <p style="font-size:0.8rem; color:var(--text-muted); margin-bottom:0.5rem;">
                        Nhấn nút <strong>Xác Nhận</strong> ở dưới để đồng bộ trạng thái chốt cước và hoàn tất lưu HĐĐT.
                    </p>
                    <input type="hidden" id="op-resolve-action" value="LOCK_ACCOUNTS">
                `;
            }
        }

        function toggleOpValueInput() {
            const action = document.getElementById('op-resolve-action').value;
            const group = document.getElementById('op-value-group');
            if (action === 'MANUAL_INPUT' || action === 'CORRECT') {
                group.style.display = 'block';
            } else {
                group.style.display = 'none';
            }
        }

        function closeOperationPanel() {
            const modal = document.getElementById('operation-modal');
            if (modal) modal.classList.remove('active');
            activeOp = { bookId: '', type: '' };
        }

        // Submit operation
        function submitOperation() {
            if (!activeOp.bookId) return;

            const bookId = activeOp.bookId;
            const book = simData[bookId];
            const type = activeOp.type;
            const action = document.getElementById('op-resolve-action').value;

            if (type === 'readings') {
                const accId = document.getElementById('op-acc-id').value;
                const correctedVal = document.getElementById('op-corrected-val') ? document.getElementById('op-corrected-val').value : '';

                // Log Kafka Event
                const eventPayload = {
                    "accountId": accId,
                    "bookId": bookId,
                    "resolutionType": action === 'USE_FALLBACK' ? 'ACCEPT_AS_IS' : 'CORRECT',
                    "correctedIndex": action === 'USE_FALLBACK' ? 1450 : parseInt(correctedVal) || 1200,
                    "timestamp": new Date().toISOString()
                };
                consoleLog("meter-reading-resolutions", "READING_RESOLVED", eventPayload);

                // Update Local Simulation DB
                book.readingsError = Math.max(0, book.readingsError - 1);
                book.success += 1;
                if (book.readingsError === 0 && book.indexError === 0 && book.waitingCalc === 0 && book.anomalies === 0) {
                    book.status = 'SUCCESS';
                }

                alert("Đã gửi chỉ số hiệu chỉnh của khách hàng " + accId + " sang Billing System qua Kafka!");

            } else if (type === 'index') {
                const accId = document.getElementById('op-acc-id').value;
                const correctedVal = document.getElementById('op-corrected-val') ? document.getElementById('op-corrected-val').value : '';

                // Log Kafka Event
                const eventPayload = {
                    "accountId": accId,
                    "bookId": bookId,
                    "resolutionType": action,
                    "correctedIndex": action === 'ACCEPT_AS_IS' ? 1980 : parseInt(correctedVal) || 1550,
                    "timestamp": new Date().toISOString()
                };
                consoleLog("meter-reading-resolutions", "INDEX_CORRECTED", eventPayload);

                // Update Local Simulation DB
                book.indexError = Math.max(0, book.indexError - 1);
                book.success += 1;
                if (book.readingsError === 0 && book.indexError === 0 && book.waitingCalc === 0 && book.anomalies === 0) {
                    book.status = 'SUCCESS';
                }

                alert("Đã gửi lệnh duyệt xác nhận chỉ số cho " + accId + " sang Billing System!");

            } else if (type === 'anomaly') {
                const accId = document.getElementById('op-acc-id').value;

                // Log Kafka Event
                const eventPayload = {
                    "accountId": accId,
                    "bookId": bookId,
                    "status": action === 'LOCK_CMIS' ? 'READY_FOR_E_INVOICE' : 'CANCELLED_BILLING',
                    "timestamp": new Date().toISOString()
                };
                consoleLog("billing-operations-topic", "ANOMALY_OPERATION", eventPayload);

                // Update Local Simulation DB
                book.anomalies = Math.max(0, book.anomalies - 1);
                if (action === 'LOCK_CMIS') {
                    book.success += 1; // Duyệt OK chuyển thành công
                } else {
                    book.waitingCalc += 1; // Hủy tính cước -> chuyển sang hàng đợi chờ tính
                }
                if (book.readingsError === 0 && book.indexError === 0 && book.waitingCalc === 0 && book.anomalies === 0) {
                    book.status = 'SUCCESS';
                }

                alert("Đã gửi quyết định xử lý bất thường cho " + accId + " sang Billing System!");

            } else if (type === 'waiting') {
                const accId = document.getElementById('op-acc-id').value;

                // Update Local Simulation DB
                book.waitingCalc = Math.max(0, book.waitingCalc - 1);

                if (action === 'RECALCULATE') {
                    // Log Kafka Event
                    const eventPayload = {
                        "accountId": accId,
                        "bookId": bookId,
                        "action": "RUN_CALCULATION",
                        "timestamp": new Date().toISOString()
                    };
                    consoleLog("cmis-batch-requests", "BATCH_CALCULATION_REQUEST", eventPayload);

                    // Simulator: Calculation completes in background
                    setTimeout(() => {
                        consoleLog("invoice-outbound", "CALCULATION_COMPLETED", {
                            "accountId": accId,
                            "bookId": bookId,
                            "status": "SUCCESS",
                            "amountBeforeTax": 450000
                        });
                    }, 1000);

                    book.success += 1;
                    alert("Đã gửi yêu cầu chạy tính cước lại cho khách hàng " + accId + " sang Billing System!");
                } else {
                    // RETURN_TO_GATE1
                    const eventPayload = {
                        "accountId": accId,
                        "bookId": bookId,
                        "action": "RETURN_TO_CORRECTION",
                        "timestamp": new Date().toISOString()
                    };
                    consoleLog("cmis-schedules", "RETURNED_TO_CORRECTION", eventPayload);

                    book.indexError += 1;
                    alert("Đã chuyển trả khách hàng " + accId + " về Cổng 1 (Chỉ số lỗi) thành công!");
                }

                if (book.readingsError === 0 && book.indexError === 0 && book.waitingCalc === 0 && book.anomalies === 0) {
                    book.status = 'SUCCESS';
                }

            } else if (type === 'success') {
                // Call lock API (REST Simulation)
                const restUrl = "/api/v1/monitoring/billing/operation?operationType=LOCK_ACCOUNTS&bookId=" + bookId;

                consoleLog("REST-API-CALL", "POST " + restUrl, {
                    "bookId": bookId,
                    "action": "LOCK_ACCOUNTS",
                    "accountsCount": book.success
                });

                // CDC Sync event back
                setTimeout(() => {
                    consoleLog("invoice-outbound", "CDC_SYNC_LOCKED", {
                        "bookId": bookId,
                        "status": "LOCKED",
                        "recordsProcessed": book.success
                    });
                }, 1200);

                // Update Local Simulation DB
                book.status = 'LOCKED';

                alert("Đã đồng bộ trạng thái chốt cước sang Billing System qua REST API. CMIS tiến hành lưu HĐĐT và khóa sổ!");
            }

            renderSimTable();
            closeOperationPanel();
        }

        // Navigating Ultima menu
        function switchSimulatorSubTask(taskKey) {
            document.querySelectorAll('.ultima-menu-subitem').forEach(el => {
                el.classList.remove('active-subitem');
            });
            
            const schedulingCard = document.getElementById('ultima-sched-card');
            const calculationArea = document.getElementById('ultima-calc-area');
            
            if (taskKey === 'sched') {
                const schedBtn = document.getElementById('subitem-sched');
                if (schedBtn) schedBtn.classList.add('active-subitem');
                schedulingCard.style.display = 'block';
                calculationArea.style.display = 'none';
                renderSchedTable();
            } else {
                const calcBtn = document.getElementById('subitem-calc');
                if (calcBtn) calcBtn.classList.add('active-subitem');
                schedulingCard.style.display = 'none';
                calculationArea.style.display = 'block';
                renderSimTable();
            }
        }

        function updateSimFormSched() {
            const bookId = document.getElementById('sim-book-id-sched').value;
            const displayEl = document.getElementById('selected-gcs-display-sched');
            if (displayEl) {
                displayEl.innerText = bookId;
            }
        }

        function triggerScheduleSimulationSched() {
            const schedBook = document.getElementById('sim-book-id-sched').value;
            const schedDate = document.getElementById('sim-sched-date-sched').value;
            document.getElementById('sim-book-id').value = schedBook;
            document.getElementById('sim-sched-date').value = schedDate;
            triggerScheduleSimulation();
            updateSimForm();
            switchSimulatorSubTask('calc');
        }

        function renderSchedTable() {
            const tbody = document.getElementById('sim-sched-table-body');
            if (!tbody) return;
            tbody.innerHTML = '';

            let count = 0;
            for (const bookId in simData) {
                const book = simData[bookId];
                count++;
                const tr = document.createElement('tr');

                const isScheduled = book.scheduled;
                const dateVal = isScheduled ? document.getElementById('sim-sched-date').value : "Chưa lập lịch";
                const statusBadge = isScheduled 
                    ? `<span class="badge badge-success">ĐÃ ĐỒNG BỘ KAFKA</span>` 
                    : `<span class="badge badge-warning">CHƯA LẬP LỊCH</span>`;

                tr.innerHTML = `
                    <td><input type="checkbox" style="accent-color:#3F51B5;" ${bookId === document.getElementById('sim-book-id-sched').value ? 'checked' : ''}></td>
                    <td><strong>${bookId}</strong></td>
                    <td>${book.name}</td>
                    <td>06/2026 - Kỳ 3</td>
                    <td style="font-weight: 600;">${dateVal}</td>
                    <td>${book.kh} KH</td>
                    <td>${statusBadge}</td>
                `;
                tbody.appendChild(tr);
            }
            const countEl = document.getElementById('sim-sched-table-count');
            if (countEl) countEl.innerText = "Tổng số: " + count + " bản ghi";
        }

        function resetSimulatorState() {
            simData = {
                'SO_Y1': { name: "đối tượng quản lý TBA Yên Hòa 1", kh: 50, readingsError: 0, indexError: 0, waitingCalc: 0, anomalies: 0, success: 0, status: 'READY', scheduled: false },
                'SO_DV2': { name: "đối tượng quản lý TBA Dịch Vọng 2", kh: 80, readingsError: 0, indexError: 0, waitingCalc: 0, anomalies: 0, success: 0, status: 'READY', scheduled: false },
                'SO_CG3': { name: "đối tượng quản lý TBA Cầu Giấy 3", kh: 120, readingsError: 0, indexError: 0, waitingCalc: 0, anomalies: 0, success: 0, status: 'READY', scheduled: false }
            };
            renderSimTable();
            renderSchedTable();
            consoleLog("system", "RESET_SIMULATOR_STATE", { "status": "READY", "timestamp": new Date().toISOString() });
        }

        // ================== MENU 4: MÔ HÌNH TRIỂN KHAI ==================
        const deployNodeMapping = {
            general: {
                mediation: {
                    title: "Mediation Service & Batch Master (Unified Control Plane)",
                    tag: "Spring Boot App + Spring Batch",
                    desc: "Dịch vụ thu thập chỉ số và điều phối chốt cước hợp nhất. Nhận chỉ số thô từ Kafka topic 'meter-readings-input', rà soát 3 cổng ngoại lệ (chỉ số ready, cảnh báo sản lượng). Khi nhận lệnh chốt cước từ CMIS hoặc tự động hoàn tất Sổ, dịch vụ kích hoạt Spring Batch phân chia 2,000,000 tài khoản thành các Chunks đẩy qua Kafka 'billing-execution-topic'.",
                    code: "server:\n  port: 8080\nspring:\n  batch:\n    job:\n      enabled: false"
                },
                snapshot: {
                    title: "Snapshot Generator (Redis Cache Prewarmer)",
                    tag: "Spring Boot App",
                    desc: "Dịch vụ quét và đóng băng dữ liệu hợp đồng khách hàng & bảng giá bậc thang từ PostgreSQL CSDL, nạp sẵn (Warming) lên Redis Cluster trước khi chốt cước để Worker truy vấn dưới 1ms.",
                    code: "server:\n  port: 8082\nspring:\n  data:\n    redis:\n      host: ${REDIS_HOST:localhost}"
                },
                kafka: {
                    title: "Apache Kafka Event Broker Cluster",
                    tag: "Event Spine Hub",
                    desc: "Xương sống truyền thông điệp bất đồng bộ. Các topic chính: 'cmis-schedules' (lập lịch), 'billing-execution-topic' (phân phối task tính cước cho worker) và 'invoice-outbound' (đồng bộ hóa đơn điện tử về CMIS).",
                    code: "Topic: billing-execution-topic\nPartitions: 24\nReplication Factor: 3\nMin In-Sync Replicas: 2"
                },
                redis: {
                    title: "Redis Cluster (Distributed Snapshot Cache)",
                    tag: "In-Memory Cache",
                    desc: "Cụm Redis Cluster lưu trữ snapshot dữ liệu hợp đồng khách hàng và cây công tơ (Netting) dưới dạng JSONB. Worker sẽ đọc snapshot từ đây với độ trễ cực thấp (<2ms), giảm thiểu tối đa tải truy vấn SQL lên Database.",
                    code: "Key Schema: snapshot:{accountId}:{month}\nValue Type: JSON String\nTTL: 24 Hours (86400 seconds)"
                },
                worker: {
                    title: "Distributed Billing Workers",
                    tag: "Compute Node (Java 21)",
                    desc: "Các Worker tính toán phân tán. Sử dụng Java 21 Virtual Threads để xử lý hàng ngàn tác vụ I/O bound song song mà không nghẽn luồng. Tính cước hoàn toàn trên bộ nhớ RAM dựa vào chỉ số đính kèm task và snapshot lấy từ Redis cache.",
                    code: "spring:\n  threads:\n    virtual:\n      enabled: true\n  kafka:\n    listener:\n      concurrency: 8"
                },
                database: {
                    title: "PostgreSQL (Citus) / TiDB Cluster",
                    tag: "Storage Layer",
                    desc: "CSDL lưu trữ hóa đơn chính thức (bảng hoa_don) và sự kiện outbox (bảng su_kien_outbox). Cấu hình phân vùng Range Partitioning vật lý theo tháng để tối ưu hóa hiệu năng truy vấn.",
                    code: "CREATE TABLE hoa_don (\n    id_hoa_don VARCHAR(100),\n    ma_khang VARCHAR(50),\n    thang_chu_ky VARCHAR(20) NOT NULL,\n    ...\n) PARTITION BY RANGE (thang_chu_ky);"
                },
                cdc: {
                    title: "Debezium CDC Connector & Kafka Connect",
                    tag: "CDC Engine",
                    desc: "Bộ quét thay đổi cơ sở dữ liệu (Change Data Capture). Lắng nghe sự kiện ghi nhận hóa đơn mới từ bảng outbox_event trong database PostgreSQL và đẩy tức thời sang Kafka topic 'invoice-outbound' để CMIS thực hiện ký số.",
                    code: "connector.class: io.debezium.connector.postgresql.PostgresConnector\ntable.include.list: public.su_kien_outbox\nplugin.name: pgoutput"
                }
            },
            k8s: {
                mediation: {
                    title: "Mediation & Batch Master Pods (Deployment)",
                    tag: "K8s Deployment",
                    desc: "Cụm Pod thu thập chỉ số và điều phối Spring Batch hợp nhất. Được triển khai dưới dạng Deployment vô trạng thái, hỗ trợ tự động phục hồi Pods và kích hoạt Actuator Health Probes.",
                    code: "spec:\n  replicas: 2\n  template:\n    spec:\n      containers:\n      - name: billing-mediation\n        image: mediation-service:latest"
                },
                snapshot: {
                    title: "Snapshot Generator Pods (Deployment)",
                    tag: "K8s Deployment",
                    desc: "Pod quét và nạp dữ liệu hợp đồng lên Redis Cache trước mỗi chu kỳ chốt cước. Giúp Worker Pods đạt tốc độ tính toán tối đa không bị nghẽn SQL.",
                    code: "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: snapshot-generator\nspec:\n  replicas: 1"
                },
                cdc: {
                    title: "Debezium CDC Connector Pods (Kafka Connect Cluster)",
                    tag: "K8s Deployment",
                    desc: "Cụm Pod Kafka Connect chạy Debezium Connector. Đọc WAL Log của PostgreSQL/TiDB để stream các sự kiện sinh hóa đơn mới sang Kafka topic 'invoice-outbound' nhằm mục đích đồng bộ về CMIS lõi.",
                    code: "apiVersion: kafka.strimzi.io/v1beta2\nkind: KafkaConnector\nspec:\n  class: io.debezium.connector.postgresql.PostgresConnector\n  tasksMax: 2"
                },
                kafka: {
                    title: "Strimzi Kafka Cluster (StatefulSet)",
                    tag: "Kafka Operator",
                    desc: "Cụm Kafka gồm 3 Broker được quản lý bởi Strimzi Operator để duy trì tính sẵn sàng cao, sử dụng Persistent Volume Claim (PVC) gắn trực tiếp với SAN Storage tốc độ cao.",
                    code: "apiVersion: kafka.strimzi.io/v1beta2\nkind: Kafka\nspec:\n  kafka:\n    replicas: 3\n    storage:\n      type: persistent-claim"
                },
                redis: {
                    title: "Redis Cluster Pods (StatefulSet)",
                    tag: "Redis Operator",
                    desc: "Cụm cache gồm 3 master và 3 replica chạy phân tán dưới dạng StatefulSet. Snapshot dữ liệu hộ dân được lưu phân tán trên bộ nhớ RAM các Pod này để Worker truy vấn nhanh.",
                    code: "redis-cli --cluster create 10.244.1.50:6379 10.244.2.50:6379 10.244.3.50:6379 ..."
                },
                db: {
                    title: "PostgreSQL Citus / TiDB StatefulSet",
                    tag: "K8s StatefulSet",
                    desc: "Cơ sở dữ liệu lưu trữ bền vững. Được cấu hình backup tự động qua K8s CronJob, gắn Disk SSD NVMe để phục vụ việc bulk write hóa đơn chốt cước nhanh chóng.",
                    code: "volumeMounts:\n- name: pg-storage\n  mountPath: /var/lib/postgresql/data"
                },
                worker: {
                    title: "Billing Worker Pods (KEDA Scaled)",
                    tag: "Autoscaled Pods",
                    desc: "Cụm Worker tính toán phân tán. Sử dụng Java 21 Virtual Threads và được điều phối replicas tự động bởi KEDA Scaler dựa trên độ trễ hàng đợi của Kafka.",
                    code: "minReplicaCount: 5\nmaxReplicaCount: 100\ntargetLag: 1000\nscaleTargetRef:\n  kind: Deployment\n  name: billing-worker"
                },
                keda: {
                    title: "KEDA Event-driven Autoscaler",
                    tag: "K8s Operator",
                    desc: "KEDA (Kubernetes Event-driven Autoscaling) liên tục thăm dò Kafka Lag của consumer group 'billing-worker-group'. Nếu số lượng tin nhắn chờ chốt cước tăng đột biến, KEDA lập tức ra lệnh tăng số lượng Worker Pod để xử lý kịp SLA chốt sổ.",
                    code: "triggers:\n- type: kafka\n  metadata:\n    topic: billing-execution-topic\n    consumerGroup: billing-worker-group\n    lagThreshold: '1000'"
                }
            },
            vm: {
                app: {
                    title: "App Integration VM 01 (Unified Mediation Service + CDC Node 1)",
                    tag: "VM Co-located Services Pair 01",
                    desc: "Máy chủ tích hợp gộp 01: Chạy dịch vụ hợp nhất Mediation Service (bao gồm Spring Batch Orchestrator Engine) và Debezium CDC Connector Node 1. Tất cả được quản lý ngầm qua Systemd (trên Linux) hoặc Windows Service qua NSSM wrapper (trên Windows).",
                    code: "# Linux Systemd Services (Khởi chạy 2 dịch vụ):\nsystemctl start billing-mediation\nsystemctl start debezium-cdc\n\n# Windows NSSM Setup:\nnssm install BillingMediation \"C:\\Java\\bin\\java.exe\" \"-jar C:\\app\\mediation.jar\"\nnssm install DebeziumCDC \"C:\\Java\\bin\\java.exe\" \"-jar C:\\app\\cdc.jar\""
                },
                sysctl: {
                    title: "App Integration VM 02 (Dự phòng HA & Tinh chỉnh OS TCP Ports)",
                    tag: "VM Co-located Services Pair 02 & OS Tuning",
                    desc: "Máy chủ tích hợp gộp 02 (Node dự phòng Active/Standby): Chạy đồng thời Mediation Instance 2 (hợp nhất Batch Engine) và Debezium CDC Node 2. Áp dụng tinh chỉnh OS Sysctl (Linux) hoặc Registry (Windows) để mở rộng dải port TCP và giảm thời gian giữ kết nối.",
                    code: "# Linux Sysctl Tuning (/etc/sysctl.conf):\nfs.file-max = 2097152\nnet.ipv4.ip_local_port_range = 1024 65535\nnet.core.somaxconn = 32768\n\n# Windows Registry TCP Parameters:\nMaxUserPort = 65534 (DWORD)\nTcpTimedWaitDelay = 30 (DWORD)"
                },
                worker: {
                    title: "Worker Server Nodes (VM 01 & 02)",
                    tag: "Systemd / Windows Service",
                    desc: "Các máy chủ Worker tính toán phân tán. Sử dụng Java 21 Virtual Threads để xử lý I/O bound song song, tối ưu hóa RAM và thực hiện bulk write. Chạy ngầm dưới dạng Systemd service (Linux) hoặc Windows Service qua NSSM.",
                    code: "# Linux Systemd Service:\nsystemctl start billing-worker\n\n# Windows NSSM Setup:\nnssm install BillingWorker \"C:\\Java\\bin\\java.exe\" \"-Xms8g -Xmx16g -jar C:\\app\\billing-worker.jar\"\nnssm start BillingWorker"
                },
                kafka: {
                    title: "Kafka Broker Cluster Nodes",
                    tag: "Event Broker Cluster",
                    desc: "Cụm Kafka gồm 3 Broker chạy trên 3 VM độc lập để đảm bảo tính sẵn sàng cao, chịu lỗi mạng tốt và phân phối partitions hợp lý.",
                    code: "# Cấu hình bootstrap.servers trong client:\nbootstrap.servers=10.0.1.30:9092,10.0.1.31:9092,10.0.1.32:9092"
                },
                redis: {
                    title: "Redis Cluster Cache Nodes",
                    tag: "Memory Cache Cluster",
                    desc: "Cụm Redis Cluster gồm 6 Nodes (3 Master + 3 Replica) chạy phân tán trên các VM để lưu trữ snapshot dữ liệu hợp đồng khách hàng.",
                    code: "# Kiểm tra trạng thái Cluster:\nredis-cli -c -h 10.0.1.40 -p 6379 cluster info"
                },
                db: {
                    title: "Database Cluster Nodes",
                    tag: "Storage Layer",
                    desc: "Cụm CSDL PostgreSQL hoặc TiDB chạy trên các VM độc lập. Triển khai theo mô hình Active/Standby để đảm bảo an toàn dữ liệu và dự phòng thảm họa.",
                    code: "# PostgreSQL Active/Standby Configuration:\nhost replication replicator 10.0.1.0/24 md5"
                }
            }
        };

        function showDeployDetail(envKey, nodeKey) {
            const data = deployNodeMapping[envKey][nodeKey];
            if (!data) return;

            // Highlight node in SVG
            document.querySelectorAll('#svg-deploy-' + envKey + ' .node-group').forEach(node => {
                node.classList.remove('active-node');
            });
            const targetNode = document.getElementById('node-dep-' + envKey + '-' + nodeKey);
            if (targetNode) {
                targetNode.classList.add('active-node');
            }

            // Fill details panel
            document.getElementById('deploy-' + envKey + '-detail-title').innerText = data.title;
            document.getElementById('deploy-' + envKey + '-detail-tag').innerText = data.tag;
            document.getElementById('deploy-' + envKey + '-detail-desc').innerText = data.desc;

            const codeWrapper = document.getElementById('deploy-' + envKey + '-code-wrapper');
            const codeEl = document.getElementById('deploy-' + envKey + '-detail-code');
            if (data.code) {
                codeWrapper.style.display = 'block';
                codeEl.innerText = data.code;
            } else {
                codeWrapper.style.display = 'none';
            }
        }

        // ================== MENU 4: MÔ HÌNH TRIỂN KHAI ==================
        function switchDeploymentView(viewId) {
            // Remove active class from all deployment sub-tab buttons
            document.getElementById('sub-tab-deploy-general').classList.remove('active');
            document.getElementById('sub-tab-deploy-k8s').classList.remove('active');
            document.getElementById('sub-tab-deploy-vm').classList.remove('active');
            document.getElementById('sub-tab-deploy-sizing').classList.remove('active');

            // Hide all sub-panels
            document.getElementById('deploy-general').style.display = 'none';
            document.getElementById('deploy-k8s').style.display = 'none';
            document.getElementById('deploy-vm').style.display = 'none';
            document.getElementById('deploy-sizing').style.display = 'none';

            // Show selected panel and set sub-tab button active
            document.getElementById('sub-tab-' + viewId).classList.add('active');
            document.getElementById(viewId).style.display = 'block';

            if (viewId === 'deploy-sizing') {
                updateSizingResults();
            }
        }

        function updateSizingResults() {
            const customerKey = document.getElementById('sizing-customers').value;
            const envElements = document.getElementsByName('sizing-env-radio');
            let env = 'k8s';
            for (let i = 0; i < envElements.length; i++) {
                if (envElements[i].checked) {
                    env = envElements[i].value;
                    break;
                }
            }

            const slaTime = document.getElementById('sizing-sla-time').value;
            let T = 7200; // SLA chuẩn 2h = 7200s
            let timeName = "2 giờ";
            if (slaTime === '1h') { T = 3600; timeName = "1 giờ"; }
            else if (slaTime === '2h') { T = 7200; timeName = "2 giờ"; }
            else if (slaTime === '4h') { T = 14400; timeName = "4 giờ"; }
            else if (slaTime === '8h') { T = 28800; timeName = "8 giờ"; }

            // Quy mô khách hàng N
            let N = 2000000;
            let customerName = "2 Triệu Khách hàng (Test)";
            if (customerKey === '100k') { N = 100000; customerName = "100k Khách hàng"; }
            else if (customerKey === '500k') { N = 500000; customerName = "500k Khách hàng"; }
            else if (customerKey === '1m') { N = 1000000; customerName = "1 Triệu Khách hàng"; }
            else if (customerKey === '2m') { N = 2000000; customerName = "2 Triệu Khách hàng (Môi trường Test)"; }
            else if (customerKey === '5m') { N = 5000000; customerName = "5 Triệu Khách hàng"; }
            else if (customerKey === '10m') { N = 10000000; customerName = "10 Triệu Khách hàng"; }

            // Áp dụng công thức SLA chốt cước: R_calc = (N / T) * F_burst (F_burst = 1.5)
            const F_burst = 1.5;
            const R_calc = Math.ceil((N / T) * F_burst);
            let formattedThroughput = R_calc.toLocaleString('vi-VN') + " HĐ/s";
            
            // Quy đổi lưu lượng cho các thành phần (theo báo cáo nghiên cứu)
            const R_kafka = R_calc * 20; // 20 messages/s cho mỗi tài khoản (readings, calculate, invoice, outbox, etc.)
            const R_db = R_calc * 3;     // 3 DB writes/s (hóa đơn, nhật ký ghi, outbox log)
            const R_redis = R_calc * 10; // 10 cache ops/s (đọc metadata hợp đồng, bảng giá bậc thang, netting tree)

            // Băng thông mạng nội bộ tối thiểu cần thiết
            let bandwidthMbps = (R_kafka * 1.5 * 8) / 1024; // Giả định kích thước tin nhắn trung bình 1.5 KB
            let formattedBandwidth = bandwidthMbps >= 1000 
                ? (bandwidthMbps / 1024).toFixed(1) + " Gbps" 
                : Math.ceil(bandwidthMbps) + " Mbps";

            let results = {};

            if (env === 'k8s') {
                // --- MÔI TRƯỜNG KUBERNETES (K8S) ---
                
                // 1. KAFKA BROKERS
                // Brokers = Max(3, Ceil(R_kafka / 10000))
                let kafkaPods = Math.max(3, Math.ceil(R_kafka / 15000));
                let kafkaCpu = (N <= 500000) ? 2 : ((N <= 5000000) ? 8 : 16);
                let kafkaRam = kafkaCpu * 2; // Tỷ lệ RAM/vCPU là 2:1 cho Page Cache OS
                // Sizing SSD dựa trên dữ liệu đệm 48h
                let kafkaSsd = Math.max(20, Math.ceil((N * 0.00006) / kafkaPods)); 

                results.kafka = {
                    nodes: `${kafkaPods} Pods (chạy ${kafkaPods} Kafka Brokers)`,
                    cpu: `${kafkaCpu} vCPU`,
                    ram: `${kafkaRam} GB`,
                    ssd: kafkaSsd >= 1000 ? `${(kafkaSsd/1000).toFixed(1)} TB NVMe` : `${kafkaSsd} GB NVMe`
                };

                // 2. REDIS CLUSTER CACHE
                // RAM_total = N * 4.08 KB * 2 replicas * 1.1 frag * 1.3 headroom
                let ramTotalGb = (N * 4.08 / 1024 / 1024) * 2 * 1.1 * 1.3;
                let redisMasters = Math.max(3, Math.ceil(R_redis / 15000));
                let redisPods = redisMasters * 2; // Gồm Master + Replica
                
                let redisRam = Math.max(2, Math.ceil(ramTotalGb / redisPods));
                let redisCpu = (N <= 500000) ? 1 : 4;
                let redisSsd = redisRam * 2; // Lưu AOF/RDB log

                results.redis = {
                    nodes: `${redisPods} Pods (Redis Cluster: ${redisMasters} Master + ${redisMasters} Replica)`,
                    cpu: `${redisCpu} vCPU`,
                    ram: `${redisRam} GB`,
                    ssd: `${redisSsd} GB NVMe`
                };

                // 3. DATABASE CLUSTER (PostgreSQL Citus / TiDB)
                // Nodes = Max(2, Ceil(R_db / 1000))
                let dbPods = (N <= 100000) ? 1 : Math.max(2, Math.ceil(R_db / 1500));
                let dbCpu = (N <= 500000) ? 4 : 16;
                let dbRam = dbCpu * 2; // K8s pod buffer pool
                
                // SSD cho 3 tháng dữ liệu đệm tính toán (sau đó sync về CMIS lõi): N * 15 KB * 3 replicas * 1.5 index/WAL overhead
                let dbStorageGb = (N * 15 / 1024 / 1024) * 3 * 1.5;
                let dbSsdPerNode = Math.max(50, Math.ceil(dbStorageGb / dbPods));

                let dbRole = dbPods === 1 ? "Độc lập" : (dbPods === 2 ? "Active/Standby" : "Citus/TiDB Cluster");
                results.db = {
                    nodes: `${dbPods} Pods (${dbRole})`,
                    cpu: `${dbCpu} vCPU`,
                    ram: `${dbRam} GB`,
                    ssd: dbSsdPerNode >= 1000 ? `${(dbSsdPerNode/1000).toFixed(1)} TB NVMe` : `${dbSsdPerNode} GB NVMe`
                };

                // 4. APP INTEGRATION CLUSTER (Mediation + Orchestrator + CDC)
                let appPods = (N <= 5000000) ? 2 : 4;
                results.appIntegration = {
                    nodes: `${appPods} Pods (Co-located Integration Cluster)`,
                    cpu: "4 vCPU",
                    ram: "8 GB",
                    ssd: "50 GB NVMe"
                };

                // 5. BILLING WORKERS (COMPUTE LAYER)
                // Mỗi vCPU core xử lý thực tế 15 HĐ phức tạp/giây. Đảm bảo tối thiểu 2 Pods Active/Active.
                let totalWorkerCores = R_calc / 15;
                let workerCpuPerPod = (N <= 500000) ? 2 : 8;
                let workerPods = Math.max(2, Math.ceil(totalWorkerCores / workerCpuPerPod));
                let workerRam = workerCpuPerPod * 2;

                results.worker = {
                    nodes: `${workerPods} Pods (Billing Compute Active/Active)`,
                    cpu: `${workerCpuPerPod} vCPU`,
                    ram: `${workerRam} GB`,
                    ssd: "50 GB NVMe"
                };

            } else {
                // --- MÔI TRƯỜNG VM (MÁY CHỦ VẬT LÝ / VM) ---
                
                // 1. KAFKA BROKERS
                let kafkaVMs = Math.max(3, Math.ceil(R_kafka / 25000));
                if (kafkaVMs > 3 && kafkaVMs % 2 === 0) kafkaVMs++; // Đảm bảo số lẻ để bầu quorum
                let kafkaCpu = (N <= 500000) ? 4 : ((N <= 5000000) ? 16 : 32);
                let kafkaRam = kafkaCpu * 2;
                let kafkaSsd = Math.max(30, Math.ceil((N * 0.00008) / kafkaVMs));

                results.kafka = {
                    nodes: `${kafkaVMs} máy chủ (chạy ${kafkaVMs} Kafka Brokers Cluster)`,
                    cpu: `${kafkaCpu} vCPU`,
                    ram: `${kafkaRam} GB`,
                    ssd: kafkaSsd >= 1000 ? `${(kafkaSsd/1000).toFixed(0)} TB SSD` : `${kafkaSsd} GB SSD`
                };

                // 2. REDIS CLUSTER CACHE
                let ramTotalGb = (N * 4.08 / 1024 / 1024) * 2 * 1.1 * 1.3;
                let redisMasters = Math.max(3, Math.ceil(R_redis / 25000));
                let redisVMs = redisMasters * 2;
                
                let redisRam = Math.max(4, Math.ceil(ramTotalGb / redisVMs));
                let redisCpu = (N <= 500000) ? 2 : 8;
                let redisSsd = redisRam * 2;

                results.redis = {
                    nodes: `${redisVMs} máy chủ (Redis Cluster: ${redisMasters} Master + ${redisMasters} Replica)`,
                    cpu: `${redisCpu} vCPU`,
                    ram: `${redisRam} GB`,
                    ssd: `${redisSsd} GB SSD`
                };

                // 3. DATABASE CLUSTER (PostgreSQL Citus / TiDB)
                let dbVMs = (N <= 100000) ? 1 : Math.max(2, Math.ceil(R_db / 3000));
                let dbCpu = (N <= 500000) ? 8 : ((N <= 5000000) ? 16 : 32);
                let dbRam = dbCpu * 4; // Tỷ lệ vàng 1:4 cho database buffer cache hệ thống lớn
                
                // SSD cho 3 tháng dữ liệu đệm tính toán (sau đó sync về CMIS lõi): N * 15 KB * 3 replicas * 1.5 index/WAL overhead
                let dbStorageGb = (N * 15 / 1024 / 1024) * 3 * 1.5;
                let dbSsdPerNode = Math.max(100, Math.ceil(dbStorageGb / dbVMs));

                let dbRole = dbVMs === 1 ? "PostgreSQL Standalone" : (dbVMs === 2 ? "PostgreSQL Active/Standby" : "Citus/TiDB Cluster");
                results.db = {
                    nodes: `${dbVMs} máy chủ (${dbRole})`,
                    cpu: `${dbCpu} vCPU`,
                    ram: `${dbRam} GB`,
                    ssd: dbSsdPerNode >= 1000 ? `${(dbSsdPerNode/1000).toFixed(0)} TB SSD` : `${dbSsdPerNode} GB SSD`
                };

                // 4. APP INTEGRATION CLUSTER (Mediation + Orchestrator + CDC)
                let appVMs = (N <= 5000000) ? 2 : 4;
                results.appIntegration = {
                    nodes: `${appVMs} máy chủ (Active/Active App Integration Pair)`,
                    cpu: "8 vCPU",
                    ram: "16 GB",
                    ssd: "100 GB SSD"
                };

                // 5. BILLING WORKERS (COMPUTE LAYER)
                // Mỗi vCPU core xử lý thực tế 15 HĐ/giây. Đảm bảo tối thiểu 2 VMs Active/Active (HA).
                let totalWorkerCores = R_calc / 15;
                let workerCpuPerVM = (N <= 500000) ? 4 : 16;
                let workerVMs = Math.max(2, Math.ceil(totalWorkerCores / workerCpuPerVM));
                let workerRam = workerCpuPerVM * 2;
                let workerSsd = (N <= 500000) ? 50 : 100;

                results.worker = {
                    nodes: `${workerVMs} máy chủ (VM Compute Nodes Active/Active)`,
                    cpu: `${workerCpuPerVM} vCPU`,
                    ram: `${workerRam} GB`,
                    ssd: `${workerSsd} GB SSD`
                };
            }

            // Ghi dữ liệu vào bảng
            const tbody = document.getElementById('sizing-table-body');
            tbody.innerHTML = `
                <tr>
                    <td><strong>Distributed Kafka Broker</strong></td>
                    <td>${results.kafka.nodes}</td>
                    <td>${results.kafka.cpu}</td>
                    <td>${results.kafka.ram}</td>
                    <td>${results.kafka.ssd}</td>
                </tr>
                <tr>
                    <td><strong>Redis Cluster Cache</strong></td>
                    <td>${results.redis.nodes}</td>
                    <td>${results.redis.cpu}</td>
                    <td>${results.redis.ram}</td>
                    <td>${results.redis.ssd}</td>
                </tr>
                <tr>
                    <td><strong>PostgreSQL / TiDB Cluster</strong></td>
                    <td>${results.db.nodes}</td>
                    <td>${results.db.cpu}</td>
                    <td>${results.db.ram}</td>
                    <td>${results.db.ssd}</td>
                </tr>
                <tr>
                    <td><strong>App Integration Cluster (Mediation + Orchestrator + CDC)</strong></td>
                    <td>${results.appIntegration.nodes}</td>
                    <td>${results.appIntegration.cpu}</td>
                    <td>${results.appIntegration.ram}</td>
                    <td>${results.appIntegration.ssd}</td>
                </tr>
                <tr>
                    <td><strong>Billing Workers (Compute)</strong></td>
                    <td><strong style="color:var(--primary);">${results.worker.nodes}</strong></td>
                    <td>${results.worker.cpu}</td>
                    <td>${results.worker.ram}</td>
                    <td>${results.worker.ssd}</td>
                </tr>
            `;

            // Cập nhật thẻ tóm tắt
            const envName = env === 'k8s' ? "Kubernetes (K8s)" : "VM (Linux/Windows)";
            const summaryCard = document.getElementById('sizing-summary-card');
            
            summaryCard.innerHTML = `
                <h4 style="color:#1e1b4b; font-weight:700;">📝 Nhận Xét &amp; Gợi Ý Triển Khai (${customerName})</h4>
                <p style="margin-top:0.4rem;">
                    Đối với quy mô <strong>${customerName}</strong>, yêu cầu hoàn thành chốt cước trong <strong>${timeName}</strong> trên môi trường <strong>${envName}</strong>:
                </p>
                <ul style="padding-left: 1.2rem; margin-top:0.3rem; display:flex; flex-direction:column; gap:0.25rem;">
                    <li>Hiệu năng chốt cước tối ưu (Throughput trung bình) cần đạt khoảng: <strong style="color:var(--success); font-size:0.9rem;">${formattedThroughput}</strong></li>
                    <li>Băng thông mạng nội bộ tối thiểu khuyên dùng: <strong style="color:var(--primary-light);">${formattedBandwidth}</strong></li>
                    <li>Đề xuất số lượng máy chủ/Pod được thiết kế để phân bổ tài nguyên hợp lý, đảm bảo SLA chốt sổ.</li>
                    ${env === 'k8s' ? '<li>Đề xuất kích hoạt <strong>KEDA HPA</strong> tự động scale-out số lượng Worker Pod dựa trên độ trễ hàng đợi Kafka.</li>' : ''}
                    ${env === 'vm' ? '<li>Hãy cấu hình tối ưu tham số <code>sysctl</code> (trên Linux) hoặc tinh chỉnh <code>MaxUserPort / TcpTimedWaitDelay</code> trong Registry (trên Windows Server) trước khi khởi chạy các dịch vụ để tránh nghẽn kết nối TCP.</li>' : ''}
                </ul>
            `;
        }

        // Init Sim tables
        renderSimTable();
        renderSchedTable();
        updateSimForm();
        updateSimFormSched();
        updateSizingResults();
    