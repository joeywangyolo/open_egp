"""
EGP Schema Transformer
======================
這個程式用於轉換 SAS Enterprise Guide Project (.egp) 檔案中的 schema 和 table 名稱。

主要功能：
1. 解壓縮 .egp 檔案（實際上是 ZIP 格式）
2. 解析 UTF-16 編碼的 project.xml
3. 根據對照表轉換 schema 和 table 名稱
4. 支援轉換 SQL 查詢中的表格引用
5. 產出轉換後的檔案，可用於 Databricks 等雲端資料庫

設計特點：
- 模組化設計，易於擴充
- 支援多種轉換規則
- 可擴充支援其他檔案格式
- 詳細的日誌記錄
"""

import zipfile
import xml.etree.ElementTree as ET
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod
import shutil
import tempfile


# ============================================================================
# 配置日誌
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# 資料類別定義
# ============================================================================
@dataclass
class SchemaMapping:
    """Schema 對照規則"""
    source_schema: str
    target_schema: str
    source_table: Optional[str] = None  # None 表示適用於整個 schema
    target_table: Optional[str] = None
    
    def matches(self, schema: str, table: str) -> bool:
        """檢查是否符合此對照規則"""
        schema_match = self.source_schema.upper() == schema.upper()
        if self.source_table is None:
            return schema_match
        return schema_match and self.source_table.upper() == table.upper()
    
    def transform(self, schema: str, table: str) -> Tuple[str, str]:
        """執行轉換"""
        new_schema = self.target_schema
        new_table = self.target_table if self.target_table else table
        return new_schema, new_table


@dataclass
class TransformationResult:
    """轉換結果"""
    success: bool
    output_path: Optional[Path] = None
    error_message: Optional[str] = None
    transformations_applied: int = 0
    details: Dict = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


# ============================================================================
# 抽象基類：定義轉換器介面
# ============================================================================
class BaseTransformer(ABC):
    """轉換器基類，定義所有轉換器必須實作的介面"""
    
    @abstractmethod
    def can_handle(self, file_path: Path) -> bool:
        """判斷是否能處理此檔案"""
        pass
    
    @abstractmethod
    def transform(self, input_path: Path, output_path: Path, 
                  mappings: List[SchemaMapping]) -> TransformationResult:
        """執行轉換"""
        pass


# ============================================================================
# EGP 檔案處理器
# ============================================================================
class EGPFileHandler:
    """處理 .egp 檔案的解壓縮和壓縮"""
    
    @staticmethod
    def extract(egp_path: Path, extract_to: Path) -> bool:
        """
        解壓縮 .egp 檔案
        
        Args:
            egp_path: .egp 檔案路徑
            extract_to: 解壓縮目標資料夾
            
        Returns:
            是否成功
        """
        try:
            logger.info(f"正在解壓縮: {egp_path}")
            with zipfile.ZipFile(egp_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"解壓縮完成: {extract_to}")
            return True
        except Exception as e:
            logger.error(f"解壓縮失敗: {e}")
            return False
    
    @staticmethod
    def compress(source_dir: Path, egp_path: Path) -> bool:
        """
        壓縮資料夾為 .egp 檔案
        
        Args:
            source_dir: 來源資料夾
            egp_path: 輸出 .egp 檔案路徑
            
        Returns:
            是否成功
        """
        try:
            logger.info(f"正在壓縮: {source_dir} -> {egp_path}")
            with zipfile.ZipFile(egp_path, 'w', zipfile.ZIP_DEFLATED) as zip_ref:
                for file_path in source_dir.rglob('*'):
                    if file_path.is_file():
                        arcname = file_path.relative_to(source_dir)
                        zip_ref.write(file_path, arcname)
            logger.info(f"壓縮完成: {egp_path}")
            return True
        except Exception as e:
            logger.error(f"壓縮失敗: {e}")
            return False


# ============================================================================
# XML 處理器
# ============================================================================
class XMLProcessor:
    """處理 UTF-16 編碼的 XML 檔案"""
    
    @staticmethod
    def read_xml(xml_path: Path) -> Optional[str]:
        """
        讀取 UTF-16 編碼的 XML 檔案
        
        Args:
            xml_path: XML 檔案路徑
            
        Returns:
            XML 內容字串，失敗則返回 None
        """
        try:
            # 嘗試多種編碼
            encodings = ['utf-16', 'utf-16-le', 'utf-16-be', 'utf-8']
            for encoding in encodings:
                try:
                    with open(xml_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    logger.info(f"成功使用 {encoding} 編碼讀取 XML")
                    return content
                except UnicodeDecodeError:
                    continue
            
            logger.error(f"無法讀取 XML 檔案: {xml_path}")
            return None
        except Exception as e:
            logger.error(f"讀取 XML 失敗: {e}")
            return None
    
    @staticmethod
    def write_xml(xml_path: Path, content: str, encoding: str = 'utf-16') -> bool:
        """
        寫入 XML 檔案
        
        Args:
            xml_path: XML 檔案路徑
            content: XML 內容
            encoding: 編碼格式
            
        Returns:
            是否成功
        """
        try:
            with open(xml_path, 'w', encoding=encoding) as f:
                f.write(content)
            logger.info(f"成功寫入 XML: {xml_path}")
            return True
        except Exception as e:
            logger.error(f"寫入 XML 失敗: {e}")
            return False


# ============================================================================
# SQL 轉換器
# ============================================================================
class SQLTransformer:
    """轉換 SQL 語句中的 schema 和 table 名稱"""
    
    @staticmethod
    def transform_sql(sql: str, mappings: List[SchemaMapping]) -> Tuple[str, int]:
        """
        轉換 SQL 語句
        
        Args:
            sql: 原始 SQL 語句
            mappings: Schema 對照表
            
        Returns:
            (轉換後的 SQL, 轉換次數)
        """
        transformed_sql = sql
        transformation_count = 0
        
        # 匹配 schema.table 的模式（支援多種格式）
        # 例如: WORK.TABLE_NAME, work.table_name, [WORK].[TABLE_NAME]
        patterns = [
            r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)\b',  # schema.table
            r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]\s*\.\s*\[([a-zA-Z_][a-zA-Z0-9_]*)\]',  # [schema].[table]
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, transformed_sql, re.IGNORECASE)
            replacements = []
            
            for match in matches:
                schema = match.group(1)
                table = match.group(2)
                
                # 尋找符合的對照規則
                for mapping in mappings:
                    if mapping.matches(schema, table):
                        new_schema, new_table = mapping.transform(schema, table)
                        old_ref = match.group(0)
                        
                        # 保持原始格式（是否有括號）
                        if '[' in old_ref:
                            new_ref = f'[{new_schema}].[{new_table}]'
                        else:
                            new_ref = f'{new_schema}.{new_table}'
                        
                        replacements.append((match.start(), match.end(), new_ref))
                        transformation_count += 1
                        logger.debug(f"SQL 轉換: {old_ref} -> {new_ref}")
                        break
            
            # 從後往前替換，避免位置偏移
            for start, end, new_ref in sorted(replacements, reverse=True):
                transformed_sql = transformed_sql[:start] + new_ref + transformed_sql[end:]
        
        return transformed_sql, transformation_count


# ============================================================================
# EGP 轉換器實作
# ============================================================================
class EGPTransformer(BaseTransformer):
    """EGP 檔案轉換器"""
    
    # === 需要精準轉換的 XML 標籤清單（依處理方式分類） ===
    
    # SQL 標籤：內容是完整的 SAS SQL 程式碼，用 SQLTransformer 處理
    SQL_TAGS = ['TaskCode', 'Text']
    
    # 引用標籤：內容是 SCHEMA.TABLE 格式（如 WORK.QUERY_FOR_DIGI_ORDERS）
    REF_TAGS = ['Label', 'InputTableName']
    
    # Schema 標籤：內容只有 schema 名稱（如 WORK、DW_ENCPT）
    SCHEMA_TAGS = ['LibraryName']
    
    def __init__(self):
        self.file_handler = EGPFileHandler()
        self.xml_processor = XMLProcessor()
        self.sql_transformer = SQLTransformer()
    
    def can_handle(self, file_path: Path) -> bool:
        """檢查是否為 .egp 檔案"""
        return file_path.suffix.lower() == '.egp'
    
    def _transform_project_xml(
        self, xml_content: str, mappings: List[SchemaMapping]
    ) -> Tuple[str, int, Dict]:
        """
        精準轉換 project.xml 中各類標籤的 schema/table 引用。
        
        轉換策略（三種標籤類型）：
        1. SQL 標籤（TaskCode, Text）：用 SQLTransformer 處理完整 SQL
        2. 引用標籤（Label, InputTableName）：內容是 SCHEMA.TABLE，直接替換
        3. Schema 標籤（LibraryName）：內容只有 schema 名稱，替換 schema
        
        Args:
            xml_content: 完整的 XML 字串
            mappings: Schema 對照表
            
        Returns:
            (轉換後的 XML, 總轉換次數, 詳細資訊 dict)
        """
        transformed_xml = xml_content
        total_count = 0
        tag_details = {}
        
        # ---- 1. 處理 SQL 標籤（TaskCode, Text） ----
        for tag in self.SQL_TAGS:
            pattern = re.compile(
                rf'(<{tag}>)(.*?)(</{tag}>)',
                re.DOTALL
            )
            
            tag_count = 0
            block_index = 0
            
            def _replace_sql_block(match: re.Match) -> str:
                """替換 SQL 區塊內的 schema/table 引用"""
                nonlocal tag_count, block_index
                block_index += 1
                
                open_tag = match.group(1)
                content = match.group(2)
                close_tag = match.group(3)
                
                transformed_content, count = self.sql_transformer.transform_sql(
                    content, mappings
                )
                tag_count += count
                
                if count > 0:
                    logger.info(
                        f"  <{tag}> 區塊 #{block_index}: 轉換 {count} 處"
                    )
                
                return open_tag + transformed_content + close_tag
            
            transformed_xml = pattern.sub(_replace_sql_block, transformed_xml)
            total_count += tag_count
            
            tag_details[tag] = {
                'blocks_found': block_index,
                'transformations': tag_count
            }
            logger.info(
                f"標籤 <{tag}>: 找到 {block_index} 個區塊, "
                f"轉換 {tag_count} 處"
            )
        
        # ---- 2. 處理引用標籤（Label, InputTableName） ----
        # 內容格式為 SCHEMA.TABLE，例如 <Label>WORK.QUERY_FOR_DIGI_ORDERS</Label>
        for tag in self.REF_TAGS:
            pattern = re.compile(
                rf'(<{tag}>)([^<]+)(</{tag}>)'
            )
            
            tag_count = 0
            block_index = 0
            
            def _replace_ref(match: re.Match) -> str:
                """替換 SCHEMA.TABLE 引用"""
                nonlocal tag_count, block_index
                block_index += 1
                
                open_tag = match.group(1)
                content = match.group(2).strip()
                close_tag = match.group(3)
                
                # 檢查是否為 SCHEMA.TABLE 格式
                ref_match = re.match(
                    r'^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$',
                    content
                )
                if not ref_match:
                    return match.group(0)  # 不是 schema.table 格式，不動
                
                schema = ref_match.group(1)
                table = ref_match.group(2)
                
                for mapping in mappings:
                    if mapping.matches(schema, table):
                        new_schema, new_table = mapping.transform(schema, table)
                        new_content = f'{new_schema}.{new_table}'
                        tag_count += 1
                        logger.debug(
                            f"  <{tag}>: {content} -> {new_content}"
                        )
                        return open_tag + new_content + close_tag
                
                return match.group(0)  # 對照表沒有，不動
            
            transformed_xml = pattern.sub(_replace_ref, transformed_xml)
            total_count += tag_count
            
            tag_details[tag] = {
                'blocks_found': block_index,
                'transformations': tag_count
            }
            logger.info(
                f"標籤 <{tag}>: 找到 {block_index} 個區塊, "
                f"轉換 {tag_count} 處"
            )
        
        # ---- 3. 處理 Schema 標籤（LibraryName） ----
        # 內容只有 schema 名稱，例如 <LibraryName>WORK</LibraryName>
        for tag in self.SCHEMA_TAGS:
            pattern = re.compile(
                rf'(<{tag}>)([^<]+)(</{tag}>)'
            )
            
            tag_count = 0
            block_index = 0
            
            def _replace_schema(match: re.Match) -> str:
                """替換 schema 名稱"""
                nonlocal tag_count, block_index
                block_index += 1
                
                open_tag = match.group(1)
                schema_name = match.group(2).strip()
                close_tag = match.group(3)
                
                # 在對照表中找匹配的 source_schema
                for mapping in mappings:
                    if mapping.source_schema.upper() == schema_name.upper():
                        new_schema = mapping.target_schema
                        tag_count += 1
                        logger.debug(
                            f"  <{tag}>: {schema_name} -> {new_schema}"
                        )
                        return open_tag + new_schema + close_tag
                
                return match.group(0)  # 對照表沒有，不動
            
            transformed_xml = pattern.sub(_replace_schema, transformed_xml)
            total_count += tag_count
            
            tag_details[tag] = {
                'blocks_found': block_index,
                'transformations': tag_count
            }
            logger.info(
                f"標籤 <{tag}>: 找到 {block_index} 個區塊, "
                f"轉換 {tag_count} 處"
            )
        
        return transformed_xml, total_count, tag_details
    
    def transform(self, input_path: Path, output_path: Path, 
                  mappings: List[SchemaMapping]) -> TransformationResult:
        """
        轉換 EGP 檔案
        
        流程：
        1. 解壓縮 .egp 檔案到臨時目錄
        2. 讀取 project.xml
        3. 精準定位 <TaskCode> 區塊，只轉換其中的 schema/table 引用
        4. 轉換所有 .log 檔案中的 SQL
        5. 重新壓縮為新的 .egp 檔案
        
        Args:
            input_path: 輸入 .egp 檔案路徑
            output_path: 輸出 .egp 檔案路徑
            mappings: Schema 對照表
            
        Returns:
            轉換結果
        """
        temp_dir = None
        try:
            # 建立臨時目錄
            temp_dir = Path(tempfile.mkdtemp(prefix='egp_transform_'))
            extract_dir = temp_dir / 'extracted'
            extract_dir.mkdir()
            
            # 1. 解壓縮
            if not self.file_handler.extract(input_path, extract_dir):
                return TransformationResult(
                    success=False,
                    error_message="解壓縮失敗"
                )
            
            # 2. 精準轉換 project.xml 中的 <TaskCode> 區塊
            project_xml = extract_dir / 'project.xml'
            xml_transformations = 0
            xml_tag_details = {}
            
            if project_xml.exists():
                xml_content = self.xml_processor.read_xml(project_xml)
                if xml_content:
                    logger.info("開始精準解析 project.xml ...")
                    transformed_xml, count, xml_tag_details = (
                        self._transform_project_xml(xml_content, mappings)
                    )
                    xml_transformations = count
                    self.xml_processor.write_xml(project_xml, transformed_xml)
                    logger.info(
                        f"project.xml 轉換完成: {count} 處"
                    )
            
            # 3. 轉換所有 .log 檔案
            log_transformations = 0
            log_files = list(extract_dir.rglob('*.log'))
            
            for log_file in log_files:
                try:
                    # 讀取 log 檔案（通常是 UTF-8 或 ASCII）
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        log_content = f.read()
                    
                    # 轉換 SQL
                    transformed_log, count = self.sql_transformer.transform_sql(
                        log_content, mappings
                    )
                    log_transformations += count
                    
                    # 寫回
                    if count > 0:
                        with open(log_file, 'w', encoding='utf-8') as f:
                            f.write(transformed_log)
                        logger.info(f"轉換 log 檔案: {log_file.name} ({count} 處)")
                except Exception as e:
                    logger.warning(f"處理 log 檔案失敗 {log_file}: {e}")
            
            # 4. 壓縮為新的 .egp 檔案
            if not self.file_handler.compress(extract_dir, output_path):
                return TransformationResult(
                    success=False,
                    error_message="壓縮失敗"
                )
            
            total_transformations = xml_transformations + log_transformations
            
            return TransformationResult(
                success=True,
                output_path=output_path,
                transformations_applied=total_transformations,
                details={
                    'xml_transformations': xml_transformations,
                    'xml_tag_details': xml_tag_details,
                    'log_transformations': log_transformations,
                    'log_files_processed': len(log_files)
                }
            )
            
        except Exception as e:
            logger.error(f"轉換過程發生錯誤: {e}", exc_info=True)
            return TransformationResult(
                success=False,
                error_message=str(e)
            )
        finally:
            # 清理臨時目錄
            if temp_dir and temp_dir.exists():
                try:
                    shutil.rmtree(temp_dir)
                except Exception as e:
                    logger.warning(f"清理臨時目錄失敗: {e}")


# ============================================================================
# 對照表管理器
# ============================================================================
class MappingManager:
    """管理 Schema 對照表"""
    
    @staticmethod
    def load_from_json(json_path: Path) -> List[SchemaMapping]:
        """
        從 JSON 檔案載入對照表
        
        JSON 格式範例：
        {
            "mappings": [
                {
                    "source_schema": "WORK",
                    "target_schema": "bronze",
                    "source_table": "QUERY_FOR_DIGI_ORDERS_0000",
                    "target_table": "digi_orders"
                },
                {
                    "source_schema": "WORK",
                    "target_schema": "bronze"
                }
            ]
        }
        
        Args:
            json_path: JSON 檔案路徑
            
        Returns:
            SchemaMapping 列表
        """
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            mappings = []
            for item in data.get('mappings', []):
                mapping = SchemaMapping(
                    source_schema=item['source_schema'],
                    target_schema=item['target_schema'],
                    source_table=item.get('source_table'),
                    target_table=item.get('target_table')
                )
                mappings.append(mapping)
            
            logger.info(f"載入 {len(mappings)} 個對照規則")
            return mappings
            
        except Exception as e:
            logger.error(f"載入對照表失敗: {e}")
            return []
    
    @staticmethod
    def save_to_json(mappings: List[SchemaMapping], json_path: Path) -> bool:
        """
        儲存對照表到 JSON 檔案
        
        Args:
            mappings: SchemaMapping 列表
            json_path: JSON 檔案路徑
            
        Returns:
            是否成功
        """
        try:
            data = {
                'mappings': [
                    {
                        'source_schema': m.source_schema,
                        'target_schema': m.target_schema,
                        'source_table': m.source_table,
                        'target_table': m.target_table
                    }
                    for m in mappings
                ]
            }
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"儲存對照表成功: {json_path}")
            return True
            
        except Exception as e:
            logger.error(f"儲存對照表失敗: {e}")
            return False


# ============================================================================
# 主要轉換引擎
# ============================================================================
class TransformationEngine:
    """轉換引擎，協調所有轉換器"""
    
    def __init__(self):
        self.transformers: List[BaseTransformer] = []
        self.register_transformer(EGPTransformer())
    
    def register_transformer(self, transformer: BaseTransformer):
        """註冊新的轉換器"""
        self.transformers.append(transformer)
        logger.info(f"註冊轉換器: {transformer.__class__.__name__}")
    
    def transform_file(self, input_path: Path, output_path: Path, 
                       mappings: List[SchemaMapping]) -> TransformationResult:
        """
        轉換檔案
        
        Args:
            input_path: 輸入檔案路徑
            output_path: 輸出檔案路徑
            mappings: Schema 對照表
            
        Returns:
            轉換結果
        """
        # 尋找適合的轉換器
        for transformer in self.transformers:
            if transformer.can_handle(input_path):
                logger.info(f"使用轉換器: {transformer.__class__.__name__}")
                return transformer.transform(input_path, output_path, mappings)
        
        return TransformationResult(
            success=False,
            error_message=f"找不到適合的轉換器處理檔案: {input_path.suffix}"
        )


# ============================================================================
# 主程式入口
# ============================================================================
def main():
    """
    主程式：批次處理 input/ 資料夾中所有 .egp 檔案，結果輸出到 output/ 資料夾。
    
    資料夾結構：
        open_egp/
        ├── input/                ← 把要轉換的 .egp 檔案放這裡
        │   ├── 來自CR的線上投保.egp
        │   └── 強制險肇責攤賠.egp
        ├── output/               ← 轉換後的檔案會自動產生在這裡
        │   ├── 來自CR的線上投保.egp
        │   └── 強制險肇責攤賠.egp
        ├── schema_mapping.json
        └── egp_transformer.py
    """
    
    # 設定路徑（以程式所在目錄為基準）
    base_dir = Path(__file__).parent
    input_dir = base_dir / 'input'
    output_dir = base_dir / 'output'
    mapping_json = base_dir / 'schema_mapping.json'
    
    # 檢查資料夾是否存在，不存在就建立
    if not input_dir.exists():
        input_dir.mkdir()
        logger.info(f"已建立 input 資料夾: {input_dir}")
        logger.info("請將 .egp 檔案放入 input 資料夾後重新執行")
        return
    
    if not output_dir.exists():
        output_dir.mkdir()
        logger.info(f"已建立 output 資料夾: {output_dir}")
    
    # 掃描 input 資料夾中所有 .egp 檔案
    egp_files = list(input_dir.glob('*.egp'))
    
    if not egp_files:
        logger.error(f"input 資料夾中找不到任何 .egp 檔案: {input_dir}")
        return
    
    logger.info(f"找到 {len(egp_files)} 個 .egp 檔案")
    for f in egp_files:
        logger.info(f"  - {f.name}")
    
    # 載入對照表
    mapping_manager = MappingManager()
    mappings = mapping_manager.load_from_json(mapping_json)
    
    if not mappings:
        logger.error("未載入任何對照規則，請檢查對照表檔案")
        return
    
    # 批次轉換
    engine = TransformationEngine()
    results = []
    
    for input_egp in egp_files:
        output_egp = output_dir / input_egp.name
        
        logger.info("=" * 60)
        logger.info(f"開始轉換: {input_egp.name}")
        logger.info("=" * 60)
        
        result = engine.transform_file(input_egp, output_egp, mappings)
        results.append((input_egp.name, result))
        
        if result.success:
            logger.info(f"✅ 轉換成功: {input_egp.name}")
            logger.info(f"   輸出: {output_egp}")
            logger.info(f"   轉換: {result.transformations_applied} 處")
        else:
            logger.error(f"❌ 轉換失敗: {input_egp.name}")
            logger.error(f"   錯誤: {result.error_message}")
    
    # 總結報告
    logger.info("\n" + "=" * 60)
    logger.info("批次轉換總結")
    logger.info("=" * 60)
    
    success_count = sum(1 for _, r in results if r.success)
    fail_count = len(results) - success_count
    total_transformations = sum(
        r.transformations_applied for _, r in results if r.success
    )
    
    logger.info(f"總共處理: {len(results)} 個檔案")
    logger.info(f"成功: {success_count} 個")
    logger.info(f"失敗: {fail_count} 個")
    logger.info(f"總轉換次數: {total_transformations} 處")
    
    for filename, result in results:
        status = "✅" if result.success else "❌"
        detail = (
            f"{result.transformations_applied} 處"
            if result.success
            else result.error_message
        )
        logger.info(f"  {status} {filename} ({detail})")
    
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
