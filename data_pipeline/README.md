# Data pipeline

```bash
# Create/update feature store
make feast_apply

# Build
make build_image && make deploy_dags

# Go to airflow UI
# Set variable MLOPS_CRASH_COURSE_CODE_DIR=path/to/mlops-crash-course-code
# Run dags

# Deploy feature repo to training pipeline
make deploy_feature_repo
```

## Great Expectations validation

Great Expectations được chạy ở bước `explore_and_validate` để đảm bảo chất lượng dữ liệu đầu vào:

- Cài đặt dependencies (đã có trong `deployment/requirements.txt`, `dev_requirements.txt` và `vip_requirements.txt`).
- Chạy thử trực tiếp:

```bash
cd data_pipeline/src/db_to_offline_store
python explore_and_validate.py
```

Script sẽ đọc `data_sources/driver_stats.parquet`, kiểm tra một số điều kiện cơ bản (non-null, khoảng giá trị hợp lý). Nếu có bước nào không đạt, script sẽ raise lỗi và log chi tiết từng expectation.
