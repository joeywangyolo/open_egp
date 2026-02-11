"""
範例使用腳本
============
展示如何使用 EGP Schema Transformer 進行檔案轉換
"""

from pathlib import Path
from egp_transformer import (
    TransformationEngine,
    MappingManager,
    SchemaMapping
)
import logging


def example_1_basic_usage():
    """範例 1：基本使用方式"""
    print("\n" + "="*60)
    print("範例 1：基本使用方式")
    print("="*60)
    
    # 設定檔案路徑
    input_egp = Path(r"c:\Users\wits\Desktop\open_egp\來自CR的線上投保.egp")
    output_egp = Path(r"c:\Users\wits\Desktop\open_egp\來自CR的線上投保_transformed.egp")
    mapping_json = Path(r"c:\Users\wits\Desktop\open_egp\schema_mapping.json")
    
    # 檢查輸入檔案是否存在
    if not input_egp.exists():
        print(f"❌ 輸入檔案不存在: {input_egp}")
        return
    
    # 載入對照表
    print(f"\n📋 載入對照表: {mapping_json}")
    mapping_manager = MappingManager()
    mappings = mapping_manager.load_from_json(mapping_json)
    
    if not mappings:
        print("❌ 未載入任何對照規則")
        return
    
    print(f"✅ 成功載入 {len(mappings)} 個對照規則")
    for i, mapping in enumerate(mappings, 1):
        if mapping.source_table:
            print(f"   {i}. {mapping.source_schema}.{mapping.source_table} → "
                  f"{mapping.target_schema}.{mapping.target_table}")
        else:
            print(f"   {i}. {mapping.source_schema}.* → {mapping.target_schema}.*")
    
    # 執行轉換
    print(f"\n🔄 開始轉換...")
    print(f"   輸入: {input_egp.name}")
    print(f"   輸出: {output_egp.name}")
    
    engine = TransformationEngine()
    result = engine.transform_file(input_egp, output_egp, mappings)
    
    # 顯示結果
    print("\n" + "-"*60)
    if result.success:
        print("✅ 轉換成功！")
        print(f"   輸出檔案: {result.output_path}")
        print(f"   總共轉換: {result.transformations_applied} 處")
        print(f"   XML 轉換: {result.details.get('xml_transformations', 0)} 處")
        print(f"   Log 轉換: {result.details.get('log_transformations', 0)} 處")
        print(f"   處理檔案: {result.details.get('log_files_processed', 0)} 個")
    else:
        print("❌ 轉換失敗！")
        print(f"   錯誤訊息: {result.error_message}")
    print("-"*60)


def example_2_programmatic_mapping():
    """範例 2：程式化建立對照表"""
    print("\n" + "="*60)
    print("範例 2：程式化建立對照表")
    print("="*60)
    
    # 程式化建立對照規則
    mappings = [
        # 特定表格轉換
        SchemaMapping(
            source_schema="WORK",
            target_schema="bronze",
            source_table="QUERY_FOR_DIGI_ORDERS_0000",
            target_table="digi_orders_raw"
        ),
        SchemaMapping(
            source_schema="WORK",
            target_schema="bronze",
            source_table="QUERY_FOR_DIGI_ORDERS_0003",
            target_table="digi_orders_summary"
        ),
        # Schema 層級轉換
        SchemaMapping(
            source_schema="WORK",
            target_schema="bronze"
        ),
        SchemaMapping(
            source_schema="SASHELP",
            target_schema="reference"
        )
    ]
    
    print(f"\n✅ 建立 {len(mappings)} 個對照規則")
    
    # 儲存到 JSON 檔案
    output_json = Path(r"c:\Users\wits\Desktop\open_egp\schema_mapping_generated.json")
    mapping_manager = MappingManager()
    
    if mapping_manager.save_to_json(mappings, output_json):
        print(f"✅ 對照表已儲存: {output_json}")
    else:
        print(f"❌ 儲存對照表失敗")


def example_3_batch_processing():
    """範例 3：批次處理多個檔案"""
    print("\n" + "="*60)
    print("範例 3：批次處理多個檔案")
    print("="*60)
    
    # 定義要處理的檔案列表
    base_dir = Path(r"c:\Users\wits\Desktop\open_egp")
    
    # 尋找所有 .egp 檔案（如果存在）
    egp_files = []
    
    # 手動指定檔案（因為原始檔案已解壓縮）
    potential_files = [
        "來自CR的線上投保.egp",
        "強制險肇責攤賠.egp"
    ]
    
    for filename in potential_files:
        file_path = base_dir / filename
        if file_path.exists():
            egp_files.append(file_path)
    
    if not egp_files:
        print("❌ 找不到 .egp 檔案")
        print("   提示：請確認 .egp 檔案存在於目錄中")
        return
    
    print(f"\n📁 找到 {len(egp_files)} 個 .egp 檔案")
    
    # 載入對照表
    mapping_json = base_dir / "schema_mapping.json"
    mapping_manager = MappingManager()
    mappings = mapping_manager.load_from_json(mapping_json)
    
    if not mappings:
        print("❌ 未載入任何對照規則")
        return
    
    # 批次處理
    engine = TransformationEngine()
    results = []
    
    for input_file in egp_files:
        output_file = input_file.parent / f"{input_file.stem}_transformed{input_file.suffix}"
        
        print(f"\n🔄 處理: {input_file.name}")
        result = engine.transform_file(input_file, output_file, mappings)
        results.append((input_file.name, result))
        
        if result.success:
            print(f"   ✅ 成功 - 轉換 {result.transformations_applied} 處")
        else:
            print(f"   ❌ 失敗 - {result.error_message}")
    
    # 總結報告
    print("\n" + "="*60)
    print("批次處理總結")
    print("="*60)
    
    success_count = sum(1 for _, r in results if r.success)
    total_transformations = sum(r.transformations_applied for _, r in results if r.success)
    
    print(f"總共處理: {len(results)} 個檔案")
    print(f"成功: {success_count} 個")
    print(f"失敗: {len(results) - success_count} 個")
    print(f"總轉換次數: {total_transformations} 處")
    
    print("\n詳細結果：")
    for filename, result in results:
        status = "✅" if result.success else "❌"
        count = f"({result.transformations_applied} 處)" if result.success else f"({result.error_message})"
        print(f"   {status} {filename} {count}")


def example_4_custom_transformer():
    """範例 4：自訂轉換器（展示擴充性）"""
    print("\n" + "="*60)
    print("範例 4：自訂轉換器（展示擴充性）")
    print("="*60)
    
    from egp_transformer import BaseTransformer, TransformationResult
    
    class CustomTransformer(BaseTransformer):
        """自訂轉換器範例"""
        
        def can_handle(self, file_path: Path) -> bool:
            # 這個範例轉換器處理 .sql 檔案
            return file_path.suffix.lower() == '.sql'
        
        def transform(self, input_path: Path, output_path: Path, 
                      mappings) -> TransformationResult:
            try:
                # 讀取 SQL 檔案
                with open(input_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # 使用 SQLTransformer 進行轉換
                from egp_transformer import SQLTransformer
                transformer = SQLTransformer()
                transformed_sql, count = transformer.transform_sql(sql_content, mappings)
                
                # 寫入輸出檔案
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(transformed_sql)
                
                return TransformationResult(
                    success=True,
                    output_path=output_path,
                    transformations_applied=count
                )
            except Exception as e:
                return TransformationResult(
                    success=False,
                    error_message=str(e)
                )
    
    print("✅ 自訂轉換器已建立")
    print("   可處理檔案類型: .sql")
    print("\n如何使用：")
    print("   engine = TransformationEngine()")
    print("   engine.register_transformer(CustomTransformer())")
    print("   result = engine.transform_file(input_sql, output_sql, mappings)")


def example_5_validation():
    """範例 5：驗證對照表"""
    print("\n" + "="*60)
    print("範例 5：驗證對照表")
    print("="*60)
    
    mapping_json = Path(r"c:\Users\wits\Desktop\open_egp\schema_mapping.json")
    
    if not mapping_json.exists():
        print(f"❌ 對照表檔案不存在: {mapping_json}")
        return
    
    # 載入對照表
    mapping_manager = MappingManager()
    mappings = mapping_manager.load_from_json(mapping_json)
    
    if not mappings:
        print("❌ 對照表為空或格式錯誤")
        return
    
    print(f"✅ 對照表驗證通過")
    print(f"   檔案: {mapping_json}")
    print(f"   規則數量: {len(mappings)}")
    
    # 分析對照規則
    schema_only = [m for m in mappings if m.source_table is None]
    table_specific = [m for m in mappings if m.source_table is not None]
    
    print(f"\n📊 規則分析：")
    print(f"   Schema 層級規則: {len(schema_only)} 個")
    print(f"   特定表格規則: {len(table_specific)} 個")
    
    # 檢查潛在衝突
    print(f"\n🔍 檢查潛在衝突...")
    
    source_schemas = set()
    conflicts = []
    
    for mapping in mappings:
        key = (mapping.source_schema, mapping.source_table)
        if key in source_schemas:
            conflicts.append(key)
        source_schemas.add(key)
    
    if conflicts:
        print(f"   ⚠️  發現 {len(conflicts)} 個重複規則")
        for schema, table in conflicts:
            if table:
                print(f"      - {schema}.{table}")
            else:
                print(f"      - {schema}.*")
    else:
        print(f"   ✅ 無衝突")
    
    # 顯示所有規則
    print(f"\n📋 所有對照規則：")
    for i, mapping in enumerate(mappings, 1):
        if mapping.source_table:
            print(f"   {i}. {mapping.source_schema}.{mapping.source_table}")
            print(f"      → {mapping.target_schema}.{mapping.target_table}")
        else:
            print(f"   {i}. {mapping.source_schema}.*")
            print(f"      → {mapping.target_schema}.*")


def main():
    """主程式"""
    print("\n" + "="*60)
    print("EGP Schema Transformer - 範例使用")
    print("="*60)
    
    # 設定日誌等級
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # 選單
    examples = {
        '1': ('基本使用方式', example_1_basic_usage),
        '2': ('程式化建立對照表', example_2_programmatic_mapping),
        '3': ('批次處理多個檔案', example_3_batch_processing),
        '4': ('自訂轉換器（展示擴充性）', example_4_custom_transformer),
        '5': ('驗證對照表', example_5_validation),
    }
    
    print("\n請選擇要執行的範例：")
    for key, (desc, _) in examples.items():
        print(f"   {key}. {desc}")
    print("   0. 執行所有範例")
    print("   q. 離開")
    
    choice = input("\n請輸入選項: ").strip()
    
    if choice == 'q':
        print("\n👋 再見！")
        return
    elif choice == '0':
        print("\n🚀 執行所有範例...")
        for desc, func in examples.values():
            try:
                func()
            except Exception as e:
                print(f"\n❌ 範例執行錯誤: {e}")
    elif choice in examples:
        desc, func = examples[choice]
        print(f"\n🚀 執行範例: {desc}")
        try:
            func()
        except Exception as e:
            print(f"\n❌ 範例執行錯誤: {e}")
    else:
        print("\n❌ 無效的選項")
    
    print("\n" + "="*60)
    print("執行完成")
    print("="*60)


if __name__ == '__main__':
    main()
