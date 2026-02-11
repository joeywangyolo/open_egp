# EGP Schema Transformer

## 專案簡介

將 SAS Enterprise Guide Project (.egp) 檔案中的 schema 和 table 名稱，依照對照表轉換為雲端資料庫（如 Databricks、Snowflake）的命名規範。

## 功能特色

- **批次處理**：將 `.egp` 檔案放入 `input/` 資料夾，一次執行全部轉換，結果輸出到 `output/`
- **精準轉換**：只轉換 `<TaskCode>` 和 `<Text>` 標籤內的 SQL，不動 XML 的其他部分（metadata、路徑、系統設定等）
- **兩層過濾**：第一層鎖定 XML 標籤，第二層依對照表決定哪些 schema 要轉換
- **Docker 支援**：打包成 Docker image，可在任何環境執行
- **模組化設計**：透過繼承 `BaseTransformer` 即可擴充支援其他檔案格式
- **詳細日誌**：完整記錄每個檔案、每個區塊的轉換次數

## 資料夾結構

```
open_egp/
├── input/                  ← 放要轉換的 .egp 檔案
├── output/                 ← 轉換結果自動產出在這裡
├── egp_transformer.py      ← 主程式
├── schema_mapping.json     ← 對照表
├── Dockerfile              ← Docker 設定
├── .dockerignore
├── example_usage.py        ← 範例程式
├── requirements.txt
└── README.md
```

---

## 快速開始（本機執行）

### 系統需求

- Python 3.7+
- 標準函式庫（無需額外安裝套件）

### 步驟

1. **準備對照表** — 編輯 `schema_mapping.json`（詳見下方說明）

2. **放入檔案** — 將 `.egp` 檔案放入 `input/` 資料夾

3. **執行轉換**

```bash
python egp_transformer.py
```

4. **取得結果** — 轉換後的檔案在 `output/` 資料夾，檔名與原檔相同

---

## Docker 執行

### 1. 建立 Docker Image

```bash
cd open_egp
docker build -t egp-transformer .
```

### 2. 執行轉換

```bash
docker run --rm `
  -v "${PWD}\input:/app/input" `
  -v "${PWD}\output:/app/output" `
  egp-transformer
```

docker run --rm -v "${PWD}\input:/app/input" -v "${PWD}\output:/app/output" egp-transformer


| 參數 | 說明 |
|------|------|
| `--rm` | 執行完自動清除容器 |
| `-v ./input:/app/input` | 掛載本機 `input/` 資料夾到容器 |
| `-v ./output:/app/output` | 掛載本機 `output/` 資料夾到容器 |

### 3. 使用外部對照表（選用）

如果不同環境需要不同的對照表，可以額外掛載：

```bash
docker run --rm \
  -v ./input:/app/input \
  -v ./output:/app/output \
  -v ./schema_mapping.json:/app/schema_mapping.json \
  egp-transformer
```

### Windows 環境

Windows 的 `-v` 路徑需要用絕對路徑：

```powershell
docker run --rm `
  -v C:\Users\wits\Desktop\open_egp\input:/app/input `
  -v C:\Users\wits\Desktop\open_egp\output:/app/output `
  egp-transformer
```

---

## 對照表格式（schema_mapping.json）

### 目前的對照表

```json
{
  "description": "DW_ENCPT + WORK schema 轉換對照表",
  "mappings": [
    {
      "source_schema": "DW_ENCPT",
      "target_schema": "catalog_name.bronze",
      "source_table": "INSURANCE_CLAIM",
      "target_table": "insurance_claim"
    },
    {
      "source_schema": "DW_ENCPT",
      "target_schema": "catalog_name.bronze",
      "source_table": "DIGI_ORDERS",
      "target_table": "digi_orders"
    },
    {
      "_comment": "DW_ENCPT 通用 fallback",
      "source_schema": "DW_ENCPT",
      "target_schema": "catalog_name.bronze"
    },
    {
      "_comment": "WORK 暫存表通用規則",
      "source_schema": "WORK",
      "target_schema": "catalog_name.staging"
    }
  ]
}
```

### 規則類型

#### 完整對照（指定 table）

```json
{
  "source_schema": "DW_ENCPT",
  "target_schema": "catalog_name.bronze",
  "source_table": "INSURANCE_CLAIM",
  "target_table": "insurance_claim"
}
```

**效果：** `DW_ENCPT.INSURANCE_CLAIM` → `catalog_name.bronze.insurance_claim`

#### Schema 層級對照（不指定 table，當 fallback）

```json
{
  "source_schema": "WORK",
  "target_schema": "catalog_name.staging"
}
```

**效果：** `WORK.任何表格` → `catalog_name.staging.任何表格`（表格名稱不變）

### 優先順序

對照表**由上往下匹配，先中的優先**。建議：
1. 特定 table 的規則放前面
2. 通用 fallback 規則放最後

---

## 轉換原理

### 兩層過濾機制

```
整個 project.xml
  │
  ▼ 第一層：regex 只抓 <TaskCode> 和 <Text> 區塊
  │  （其他 XML 標籤完全不動）
  │
  ▼ 第二層：SQL 中只轉換對照表有的 schema
  │  DW_ENCPT.xxx → 轉換
  │  WORK.xxx     → 轉換（如果對照表有 WORK 規則）
  │  其他 schema  → 跳過
  │
  ▼ 輸出轉換後的 XML
```

### 轉換範圍

| 檔案 | 轉換的標籤 | 說明 |
|------|-----------|------|
| `project.xml` | `<TaskCode>` | 使用者撰寫的 SAS SQL |
| `project.xml` | `<Text>` | 查詢產生器的 SQL 副本 |
| `*.log` | 整個檔案 | 執行日誌中的 SQL |

### 不轉換的部分

| 標籤 | 內容 | 為什麼不動 |
|------|------|-----------|
| `<BeginAppCode>` | ODS/HTML 輸出設定 | 系統產生，無 schema 引用 |
| `<EndAppCode>` | quit/run/ODS CLOSE | 系統產生 |
| `<MacroAssign_Code>` | 專案路徑、機器名稱 | metadata，不是 SQL |

### 轉換範例

**轉換前：**
```sql
CREATE TABLE WORK.QUERY_FOR_INSURANCE_CLAIM_0001 AS
SELECT t1.REPORT_NUMBER, t1.INSURED_NUMBER
  FROM DW_ENCPT.INSURANCE_CLAIM t1
  WHERE t1.CLOSE_DT > '28Feb2025'd;
```

**轉換後：**
```sql
CREATE TABLE catalog_name.staging.QUERY_FOR_INSURANCE_CLAIM_0001 AS
SELECT t1.REPORT_NUMBER, t1.INSURED_NUMBER
  FROM catalog_name.bronze.insurance_claim t1
  WHERE t1.CLOSE_DT > '28Feb2025'd;
```

---

## 程式架構

### 設計模式：繼承 + 多型

```
BaseTransformer（抽象基類）
  │  定義 @abstractmethod: can_handle(), transform()
  │
  └── EGPTransformer（子類）
        覆寫 can_handle() → 只接 .egp
        覆寫 transform()  → 完整轉換邏輯
```

`TransformationEngine` 只認 `BaseTransformer` 介面，不認具體實作。新增檔案格式只需寫新的子類並註冊，不用改引擎。

### 核心類別

| 類別 | 職責 |
|------|------|
| **TransformationEngine** | 協調引擎，遍歷所有轉換器找到能處理的 |
| **EGPTransformer** | .egp 轉換：解壓 → 精準解析 XML → 轉換 SQL → 壓回 |
| **SQLTransformer** | 用 regex 匹配 `schema.table` 並依對照表替換 |
| **EGPFileHandler** | 處理 .egp 的 ZIP 解壓縮/壓縮 |
| **XMLProcessor** | 處理 UTF-16 編碼的 XML 讀寫 |
| **MappingManager** | 載入/儲存 `schema_mapping.json` |
| **SchemaMapping** | 單一對照規則的資料結構 |

### EGP 轉換流程

```
1. 解壓縮 .egp（ZIP 格式）到臨時目錄
2. 讀取 project.xml（UTF-16 編碼）
3. 用 regex 找出所有 <TaskCode> 和 <Text> 區塊
4. 對每個區塊內的 SQL，依對照表替換 schema.table
5. 轉換所有 .log 檔案中的 SQL
6. 重新壓縮為新的 .egp 檔案
7. 清理臨時目錄
```

---

## 擴充指南

### 新增檔案格式

```python
class SQLFileTransformer(BaseTransformer):
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() == '.sql'
    
    def transform(self, input_path, output_path, mappings):
        # 實作轉換邏輯
        ...

# 註冊
engine = TransformationEngine()
engine.register_transformer(SQLFileTransformer())
```

### 新增要轉換的 XML 標籤

修改 `EGPTransformer.TARGET_XML_TAGS`：

```python
TARGET_XML_TAGS = ['TaskCode', 'Text', 'BeginUserCode']
```

---

## 日誌

程式使用 Python 標準 `logging` 模組，預設輸出到控制台。

| 等級 | 內容 |
|------|------|
| **INFO** | 解壓縮、轉換進度、批次總結 |
| **WARNING** | 無法處理的檔案 |
| **ERROR** | 轉換失敗、對照表錯誤 |
| **DEBUG** | 每個 SQL 替換的細節 |

---

## 常見問題

**Q: .egp 檔案是什麼格式？**
A: 實際上是 ZIP 壓縮檔，裡面包含 `project.xml`（UTF-16 編碼）和 `.log` 等檔案。

**Q: 轉換會修改原始檔案嗎？**
A: 不會。`input/` 裡的原檔不動，結果輸出到 `output/`。

**Q: 對照表裡沒有的 schema 會怎樣？**
A: 完全跳過，不做任何修改。

**Q: 如何只轉換 DW_ENCPT，不動 WORK？**
A: 把 `schema_mapping.json` 裡的 WORK 規則刪掉即可。

**Q: Docker image 有多大？**
A: 基於 `python:3.11-slim`，約 150MB。程式只用標準函式庫，不需安裝額外套件。

---

## 授權

本專案為內部工具，請依照公司政策使用。

---

**最後更新：** 2026-02-11
**版本：** 2.0.0
