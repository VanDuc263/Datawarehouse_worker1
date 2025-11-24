from utils.mail_utils import send_error_mail
import requests
from bs4 import BeautifulSoup
import pandas as pd
import configparser
from meta_logger.meta_logger import get_logger, upload_log_to_minio
import s3fs
from datetime import datetime
import os
import psutil

# ------------------- File Status -------------------
def update_file_status(file_name, status, bucket, minio_opts):
    """Cập nhật trạng thái P1-P4 cho file trên MinIO"""
    status_file = f"s3://{bucket}/file_status.csv"
    try:
        fs = s3fs.S3FileSystem(**minio_opts["client_kwargs"],
                               key=minio_opts["key"],
                               secret=minio_opts["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=minio_opts)
        else:
            df_status = pd.DataFrame(columns=["file_name","status","last_update"])

        now = datetime.now().isoformat()
        if file_name in df_status["file_name"].values:
            df_status.loc[df_status["file_name"]==file_name, ["status","last_update"]] = [status, now]
        else:
            df_status = pd.concat([df_status, pd.DataFrame([[file_name,status,now]], columns=df_status.columns)],
                                  ignore_index=True)

        df_status.to_csv(status_file, index=False, encoding="utf-8-sig", storage_options=minio_opts)
    except Exception as e:
        print(f"❌ Lỗi cập nhật file_status.csv: {e}")


# ------------------- Process Status -------------------
def update_process_status(process_name, pid, status, bucket, minio_opts):
    """Cập nhật trạng thái process R/S/T/Z trên MinIO"""
    status_file = f"s3://{bucket}/process_status.csv"
    try:
        fs = s3fs.S3FileSystem(**minio_opts["client_kwargs"],
                               key=minio_opts["key"],
                               secret=minio_opts["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=minio_opts)
        else:
            df_status = pd.DataFrame(columns=["process_name","pid","status","last_update"])

        now = datetime.now().isoformat()
        if pid in df_status["pid"].values:
            df_status.loc[df_status["pid"]==pid, ["status","last_update"]] = [status, now]
        else:
            df_status = pd.concat([df_status, pd.DataFrame([[process_name,pid,status,now]], columns=df_status.columns)],
                                  ignore_index=True)

        df_status.to_csv(status_file, index=False, encoding="utf-8-sig", storage_options=minio_opts)
    except Exception as e:
        print(f"❌ Lỗi cập nhật process_status.csv: {e}")


# ------------------- Main ETL -------------------
def main():
    step_name = "extract"
    logger = get_logger("scraper", log_dir="logs", step_name=step_name)

    pid = os.getpid()
    process_name = "extract_process"

    try:
        # ================= Config
        config = configparser.ConfigParser()
        config.read("config.ini")

        MINIO_STORAGE_OPTIONS = {
            "key": config["MINIO"]["key"],
            "secret": config["MINIO"]["secret"],
            "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]},
        }
        bucket = config["MINIO"]["bucket"]
        raw_path = f"s3://{bucket}/raw_data.csv"
        file_name = "raw_data.csv"

        # 🔹 File P1 + Process R: File mới, process đang chạy
        logger.info(f"{file_name} - status: P1")
        update_file_status(file_name, "P1", bucket, MINIO_STORAGE_OPTIONS)
        update_process_status(process_name, pid, "R", bucket, MINIO_STORAGE_OPTIONS)

        # ================= Crawling Data
        url = "https://www.thegioididong.com/dtdd"
        headers = {"User-Agent": "Mozilla/5.0"}
        logger.info(f"🔹 Đang cào dữ liệu từ: {url}")

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        products = soup.select("ul.listproduct li")

        data = []
        for product in products:
            name = product.find("h3")
            price = product.find("strong")
            img = product.find("img")
            data.append({
                "product_name": name.text.strip() if name else None,
                "price_raw": price.text.strip() if price else None,
                "image_url": img.get("data-src") or img.get("src") if img else None,
            })

        df = pd.DataFrame(data)
        if df.empty:
            raise Exception("Không tìm thấy sản phẩm nào!")

        # 🔹 File P2 + Process R/S: Đang xử lý
        logger.info(f"{file_name} - status: P2")
        update_file_status(file_name, "P2", bucket, MINIO_STORAGE_OPTIONS)
        update_process_status(process_name, pid, "S", bucket, MINIO_STORAGE_OPTIONS)

        df.to_csv(raw_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)

        # 🔹 File P3 + Process T: Hoàn tất
        logger.info(f"{file_name} - status: P3")
        update_file_status(file_name, "P3", bucket, MINIO_STORAGE_OPTIONS)
        update_process_status(process_name, pid, "T", bucket, MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Đã lưu raw_data: {raw_path}")

        upload_log_to_minio(logger.log_file, step_name=step_name)
        return raw_path

    except Exception as e:
        # 🔹 File P4 + Process Z/Error
        logger.error(f"{file_name} - status: P4")
        update_file_status(file_name, "P4", bucket, MINIO_STORAGE_OPTIONS)
        update_process_status(process_name, pid, "Z", bucket, MINIO_STORAGE_OPTIONS)

        error_message = f"Lỗi trong bước Extract:\n{str(e)}"
        send_error_mail(
            subject="ETL ERROR - Extract Failed",
            message=error_message,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        logger.error(f"❌ Bị lỗi: {e}")
        upload_log_to_minio(logger.log_file, step_name=step_name)
        raise e


if __name__ == "__main__":
    main()

