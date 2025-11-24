import pandas as pd
from meta_logger.meta_logger import get_logger, upload_log_to_minio
import configparser, os
from datetime import datetime
import s3fs

def main():
    step_name = "transform"
    logger = get_logger("transformer", log_dir="logs", step_name=step_name)

    # ================= Config
    config_file = "config.ini"
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"{config_file} không tồn tại!")

    config = configparser.ConfigParser()
    config.read(config_file)

    MINIO_STORAGE_OPTIONS = {
        "key": config["MINIO"]["key"],
        "secret": config["MINIO"]["secret"],
        "client_kwargs": {"endpoint_url": config["MINIO"]["endpoint_url"]}
    }
    bucket = config["MINIO"]["bucket"]

    raw_path = f"s3://{bucket}/raw_data.csv"
    clean_path = f"s3://{bucket}/clean_data.csv"
    dim_brand_path = f"s3://{bucket}/dim_brand.csv"
    dim_product_path = f"s3://{bucket}/dim_product.csv"
    fact_price_path = f"s3://{bucket}/fact_product_price.csv"
    status_file = f"s3://{bucket}/file_status.csv"

    # ================= Kiểm tra file_status
    try:
        fs = s3fs.S3FileSystem(**MINIO_STORAGE_OPTIONS["client_kwargs"],
                               key=MINIO_STORAGE_OPTIONS["key"],
                               secret=MINIO_STORAGE_OPTIONS["secret"])
        if fs.exists(status_file):
            df_status = pd.read_csv(status_file, storage_options=MINIO_STORAGE_OPTIONS)
            file_status = df_status.loc[df_status["file_name"]=="raw_data.csv", "status"].values
            if len(file_status)==0 or file_status[0] != "P3":
                logger.info("🔹 raw_data.csv chưa P3 → Transform dừng.")
                return
        else:
            logger.info("🔹 Chưa có file_status.csv trên MinIO → Transform dừng.")
            return
    except Exception as e:
        logger.error(f"❌ Lỗi đọc file_status.csv: {e}")
        return

    logger.info("🔹 raw_data.csv đã P3 → Bắt đầu Transform")

    # ================= Xử lý dữ liệu
    try:
        df = pd.read_csv(raw_path, storage_options=MINIO_STORAGE_OPTIONS)
        df = df.drop_duplicates().dropna(subset=["product_name","price_raw"])
        df["price"] = df["price_raw"].str.replace("₫","").str.replace(".","").str.replace(",","").astype(float)
        df["transform_time"] = datetime.now().isoformat()

        # P1: bắt đầu Transform
        logger.info("transform - status: P1")

        df.to_csv(clean_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info(f"✅ Clean data saved: {clean_path}")
        logger.info("transform - status: P2")  # đang xử lý

        # Dim Brand
        df["brand"] = df["product_name"].apply(lambda x: x.split()[0])
        dim_brand = df[["brand"]].drop_duplicates().reset_index(drop=True)
        dim_brand["brand_id"] = dim_brand.index + 1
        dim_brand.to_csv(dim_brand_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info("✅ Tạo xong DimBrand")

        # Dim Product
        dim_product = df[["product_name","brand"]].drop_duplicates().reset_index(drop=True)
        dim_product = dim_product.merge(dim_brand, on="brand", how="left")
        dim_product["product_id"] = dim_product.index + 1
        dim_product.to_csv(dim_product_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info("✅ Tạo xong DimProduct")

        # Fact
        fact = df.merge(dim_product, on="product_name", how="left")
        fact = fact[["product_id","brand_id","price","transform_time"]]
        fact.to_csv(fact_price_path, index=False, encoding="utf-8-sig", storage_options=MINIO_STORAGE_OPTIONS)
        logger.info("📦 Đã tạo FactProductPrice")

        # P3: hoàn tất Transform
        logger.info("transform - status: P3")

    except Exception as e:
        logger.error(f"❌ Lỗi Transform: {e}")
        logger.info("transform - status: P4")

    upload_log_to_minio(logger.log_file, step_name=step_name)
    return {
        "clean_path": clean_path,
        "dim_brand_path": dim_brand_path,
        "dim_product_path": dim_product_path,
        "fact_price_path": fact_price_path
    }

if __name__ == "__main__":
    main()

