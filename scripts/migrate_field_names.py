#!/usr/bin/env python
# scripts/migrate_field_names.py

import sys
import os

# 현재 스크립트의 상위 디렉토리를 시스템 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import DatabaseManager, DB_SCHEMA
from core.logger import logger

def migrate_table_field_names():
    """기존 테이블의 필드명을 새로운 스키마에 맞게 마이그레이션"""
    print("테이블 필드명 마이그레이션 시작...")
    
    db = DatabaseManager()
    
    # DB 스키마 정보
    server_table = DB_SCHEMA.get("server_table", "servers")
    server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
    
    # 마이그레이션할 테이블 목록
    tables_to_migrate = [
        "jvm_metrics",
        "system_resources", 
        "application_impact",
        "feature_data",
        "model_performance",
        "predictions",
        "alerts",
        "job_status"
    ]
    
    migration_success = True
    
    for table_name in tables_to_migrate:
        try:
            print(f"\n=== {table_name} 테이블 마이그레이션 ===")
            
            # 1. 테이블 존재 여부 확인
            check_table_query = f"SHOW TABLES LIKE '{table_name}'"
            if not db.fetch_one(check_table_query):
                print(f"테이블 '{table_name}'이 존재하지 않습니다.")
                continue
            
            # 2. 현재 컬럼 구조 확인
            columns_query = f"""
            SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
            """
            
            columns = db.fetch_all(columns_query)
            current_columns = {col[0]: col for col in columns}
            
            # 3. server_id 컬럼이 있는지 확인
            if 'server_id' in current_columns and server_id_field not in current_columns:
                print(f"server_id 컬럼을 {server_id_field}로 변경 중...")
                
                # 외래키 제약조건 확인 및 제거
                fk_query = f"""
                SELECT CONSTRAINT_NAME 
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = '{table_name}' 
                AND COLUMN_NAME = 'server_id'
                AND REFERENCED_TABLE_NAME IS NOT NULL
                """
                
                fk_constraints = db.fetch_all(fk_query)
                
                # 외래키 제거
                for fk in fk_constraints:
                    drop_fk_query = f"ALTER TABLE {table_name} DROP FOREIGN KEY {fk[0]}"
                    if db.execute_query(drop_fk_query):
                        print(f"외래키 제약조건 {fk[0]} 제거 완료")
                    else:
                        print(f"외래키 제약조건 {fk[0]} 제거 실패")
                
                # 컬럼명 변경
                col_info = current_columns['server_id']
                column_type = col_info[1]
                nullable = "NULL" if col_info[2] == "YES" else "NOT NULL"
                default = f"DEFAULT {col_info[3]}" if col_info[3] else ""
                extra = col_info[4] if col_info[4] else ""
                
                change_column_query = f"""
                ALTER TABLE {table_name} 
                CHANGE COLUMN server_id {server_id_field} {column_type} {nullable} {default} {extra}
                """
                
                if db.execute_query(change_column_query):
                    print(f"컬럼명 변경 완료: server_id → {server_id_field}")
                    
                    # 외래키 다시 추가
                    add_fk_query = f"""
                    ALTER TABLE {table_name} 
                    ADD CONSTRAINT fk_{table_name}_{server_id_field}
                    FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field})
                    """
                    
                    if db.execute_query(add_fk_query):
                        print(f"외래키 제약조건 재추가 완료")
                    else:
                        print(f"외래키 제약조건 재추가 실패")
                        migration_success = False
                else:
                    print(f"컬럼명 변경 실패")
                    migration_success = False
            
            elif server_id_field in current_columns:
                print(f"이미 {server_id_field} 컬럼이 존재합니다.")
            else:
                print(f"server_id 컬럼이 없습니다.")
                
        except Exception as e:
            print(f"{table_name} 테이블 마이그레이션 중 오류 발생: {e}")
            migration_success = False
            import traceback
            print(traceback.format_exc())
    
    db.close()
    
    if migration_success:
        print("\n✅ 모든 테이블 마이그레이션이 성공적으로 완료되었습니다.")
    else:
        print("\n❌ 일부 테이블 마이그레이션에서 오류가 발생했습니다.")
    
    return migration_success

def verify_migration():
    """마이그레이션 결과 검증"""
    print("\n=== 마이그레이션 결과 검증 ===")
    
    db = DatabaseManager()
    server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
    
    tables_to_check = [
        "jvm_metrics", "system_resources", "application_impact", 
        "feature_data", "model_performance", "predictions", "alerts"
    ]
    
    all_ok = True
    
    for table_name in tables_to_check:
        # 테이블의 컬럼 확인
        columns_query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = '{table_name}'
        AND COLUMN_NAME = '{server_id_field}'
        """
        
        result = db.fetch_one(columns_query)
        if result:
            print(f"✅ {table_name}: {server_id_field} 컬럼 존재")
        else:
            print(f"❌ {table_name}: {server_id_field} 컬럼 없음")
            all_ok = False
    
    # 외래키 제약조건 확인
    fk_query = f"""
    SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    WHERE TABLE_SCHEMA = DATABASE() 
    AND REFERENCED_TABLE_NAME = 'servers'
    AND REFERENCED_COLUMN_NAME = '{server_id_field}'
    """
    
    fk_results = db.fetch_all(fk_query)
    print(f"\n외래키 제약조건 ({len(fk_results)}개):")
    for fk in fk_results:
        print(f"  {fk[0]}.{fk[2]} → {fk[3]}.{fk[4]}")
    
    db.close()
    
    return all_ok

if __name__ == "__main__":
    # 마이그레이션 실행
    success = migrate_table_field_names()
    
    if success:
        # 검증 수행
        verify_migration()
    else:
        print("마이그레이션 실패로 인해 검증을 건너뜁니다.")