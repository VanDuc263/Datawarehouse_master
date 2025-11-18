import configparser
from datetime import datetime

# Đọc file cấu hình
config = configparser.ConfigParser()
config.read("config.ini")

# Lấy thông tin MinIO
MINIO_STORAGE_OPTIONS = {
    "key": config["MINIO"]["key"],
    "secret": config["MINIO"]["secret"],
    "client_kwargs": {
        "endpoint_url": config["MINIO"]["endpoint_url"]
    }
}

# Lấy thông tin MySQL
MYSQL_CONFIG = {
    "user": config["MYSQL"]["user"],
    "password": config["MYSQL"]["password"],
    "host": config["MYSQL"]["host"],
    "port": int(config["MYSQL"]["port"]),
    "database": config["MYSQL"]["database"]
}

# Đường dẫn staging
today = datetime.now().strftime("%Y-%m-%d")
staging_path = f"{config['PATHS']['staging_folder']}/{today}/clean_data.csv"

print("✅ Cấu hình đã tải thành công.")
print("MinIO endpoint:", config['MINIO']['endpoint_url'])
print("MySQL host:", config['MYSQL']['host'])

