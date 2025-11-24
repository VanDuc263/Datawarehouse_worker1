import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import configparser
import s3fs

# ========================
# Đọc config MinIO từ config.ini
# ========================
config = configparser.ConfigParser()
config.read("config.ini")

MINIO_ENABLED = True  # Nếu muốn upload log lên MinIO
MINIO_BUCKET = "logs"
MINIO_ENDPOINT = config["MINIO"]["endpoint_url"]
MINIO_KEY = config["MINIO"]["key"]
MINIO_SECRET = config["MINIO"]["secret"]

# ========================
# Hàm tạo logger
# ========================
def get_logger(name="meta_logger", log_dir="logs", step_name=None,
               level=logging.INFO, max_bytes=5*1024*1024, backup_count=3):
    """
    Tạo logger chuẩn, ghi ra file và console.
    Nếu step_name được cung cấp, file log sẽ có dạng step_name-YYYY-MM-DD.log
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger  # tránh tạo handler trùng

    if step_name:
        log_file = os.path.join(log_dir, f"{step_name}-{datetime.now().strftime('%Y-%m-%d')}.log")
    else:
        log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    # File handler với rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s',
                                                '%Y-%m-%d %H:%M:%S'))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s',
                                                   '%Y-%m-%d %H:%M:%S'))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Lưu file log vào logger để dùng upload
    logger.log_file = log_file

    return logger


# ========================
# Hàm upload log lên MinIO
# ========================
def upload_log_to_minio(local_log_path, step_name=None):
    if not MINIO_ENABLED:
        return

    try:
        fs = s3fs.S3FileSystem(
            key=MINIO_KEY,
            secret=MINIO_SECRET,
            client_kwargs={"endpoint_url": MINIO_ENDPOINT}
        )
        filename = os.path.basename(local_log_path)
        if step_name:
            filename = f"{step_name}-{filename}"
        remote_path = f"s3://{MINIO_BUCKET}/{filename}"
        fs.put(local_log_path, remote_path)
        print(f"✅ Đã upload log lên MinIO: {remote_path}")
    except Exception as e:
        print(f"❌ Lỗi khi upload log lên MinIO: {e}")

