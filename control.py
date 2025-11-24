from dask.distributed import Client, wait
from workflow.workflow_tasks import (
    run_extract,
    run_transform,
    run_loaddata,
    run_checkdata,
    run_dw_load_dim_brand,
    run_dw_load_dim_product,
    run_dw_load_fact_product_price
)
import sys

sys.path.append("/root/project_dask")

client = Client("tcp://192.168.1.100:8786")
print("🔹 Connected to Dask Scheduler")

extract_path = None
transform_result = None

# =========================================================
# 🔹 1. EXTRACT
# =========================================================
print("🔹 Submitting Extract on Worker1...")
future_extract = client.submit(run_extract, workers="worker1")
wait(future_extract)

try:
    extract_path = future_extract.result(timeout=20)
    print(f"✅ Extract completed, raw data saved at: {extract_path}")
except Exception as e:
    print("❌ Extract failed")
    print("➡ Error:", e)
    print(future_extract.traceback())


# =========================================================
# 🔹 2. TRANSFORM
# =========================================================
print("\n🔹 Submitting Transform on Worker1...")
future_transform = client.submit(run_transform, workers="worker1")
wait(future_transform)

try:
    transform_result = future_transform.result(timeout=20)
    print("✅ Transform completed!")
except Exception as e:
    print("❌ Transform failed")
    print("➡ Error:", e)
    print(future_transform.traceback())


# =========================================================
# 🔹 3. LOADDATA (staging)
# =========================================================
print("\n🔹 Submitting Loaddata on Worker2...")
future_loaddata = client.submit(run_loaddata, workers="worker2")
wait(future_loaddata)

try:
    load_result = future_loaddata.result(timeout=20)
    print("✅ Loaddata completed!")
except Exception as e:
    print("❌ Loaddata failed")
    print("➡ Error:", e)
    print(future_loaddata.traceback())


# =========================================================
# 🔹 4. CHECKDATA (staging validation)
# =========================================================
print("\n🔹 Submitting Checkdata on Worker2...")
future_check = client.submit(run_checkdata, workers="worker2")
wait(future_check)

try:
    check_result = future_check.result(timeout=20)
    print("✅ Checkdata completed!")
except Exception as e:
    print("❌ Checkdata failed")
    print("➡ Error:", e)
    print(future_check.traceback())


# =========================================================
# 🔥 5. DW LOAD – dim_brand
# =========================================================
print("\n🔹 Submitting DW Load: dim_brand on Worker2...")
future_dim_brand = client.submit(run_dw_load_dim_brand, workers="worker2")
wait(future_dim_brand)

try:
    result_dim_brand = future_dim_brand.result(timeout=20)
    print("✅ DW Load dim_brand completed!")
except Exception as e:
    print("❌ DW Load dim_brand failed")
    print("➡ Error:", e)
    print(future_dim_brand.traceback())


# =========================================================
# 🔥 6. DW LOAD – dim_product
# =========================================================
print("\n🔹 Submitting DW Load: dim_product on Worker2...")
future_dim_product = client.submit(run_dw_load_dim_product, workers="worker2")
wait(future_dim_product)

try:
    result_dim_product = future_dim_product.result(timeout=20)
    print("✅ DW Load dim_product completed!")
except Exception as e:
    print("❌ DW Load dim_product failed")
    print("➡ Error:", e)
    print(future_dim_product.traceback())


# =========================================================
# 🔥 7. DW LOAD – fact_product_price
# =========================================================
print("\n🔹 Submitting DW Load: fact_product_price on Worker2...")
future_fact_price = client.submit(run_dw_load_fact_product_price, workers="worker2")
wait(future_fact_price)

try:
    result_fact_price = future_fact_price.result(timeout=20)
    print("✅ DW Load fact_product_price completed!")
except Exception as e:
    print("❌ DW Load fact_product_price failed")
    print("➡ Error:", e)
    print(future_fact_price.traceback())


# =========================================================
# 🔚 KẾT QUẢ TỔNG QUÁT
# =========================================================
print("\n⏳ ETL + DW Pipeline Finished")
print("📌 Extract Path:", extract_path)
print("📌 Transform Result:", transform_result)

