## Việc

Refs #<!-- số issue; dùng Closes # nếu PR nhắm nhánh mặc định -->

Tuần / DoD liên quan: <!-- W1 · Diff SCD2 đúng -->

## Thay đổi

-

## Cách kiểm

```bash
# lệnh đã chạy, ví dụ:
# dbt build --select stg_fppo_line+ --target dev
# pytest tests/test_diff_iud.py -q
```

Kết quả mong đợi / thực tế:

## Checklist

- [ ] Tên model/cột khớp file Mapping (bronze/silver/gold) hoặc đã ghi chỗ lệch vào wiki *Diem-lech-tai-lieu*
- [ ] Model mới có test (generic / singular / unit) và tag dbt đúng
- [ ] Idempotent theo `snapshot_id`; không watermark `ModifiedDate`, không dùng `Id` làm khoá khi mapping ghi khoá khác
- [ ] Không secret, mật khẩu, connection string có giá trị; không dữ liệu định danh khách/OC/số lượng thật
- [ ] Không có câu lệnh ghi vào DB công ty (chỉ `LoomlineDW`)
- [ ] PR `dev → uat` / `uat → main`: chọn **Create a merge commit**
