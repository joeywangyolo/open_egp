FROM python:3.11-slim

WORKDIR /app

# 複製程式碼和對照表
COPY egp_transformer.py .
COPY schema_mapping.json .

# 建立 input/output 資料夾
RUN mkdir -p /app/input /app/output

# 執行轉換
CMD ["python", "egp_transformer.py"]
