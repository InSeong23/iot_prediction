#!/usr/bin/env python
# scripts/clean_setup.py

import sys
import os
import argparse

# 현재 스크립트의 상위 디렉토리를 시스템 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import DatabaseManager
from core.logger import logger
from scripts.setup import setup_all

def drop_all_tables(db_manager, confirm=False):
    """모든 관련 테이블 삭제"""
    
    if not confirm:
        response = input("⚠️  모든 JVM 메트릭 관련 테이블을 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다! (yes/no): ")
        if response.lower() != 'yes':
            print("❌ 작업이 취소되었습니다.")
            return False
    
    print("🗑️  기존 테이블 삭제 시작...")
    
    # 삭제할 테이블 목록 (외래키 순서를 고려하여 역순으로)
    tables_to_drop = [
        "alerts",
        "predictions", 
        "model_performance",
        "feature_data",
        "application_impact",
        "system_resources",
        "jvm_metrics",
        "job_status",
        "configurations",
        "servers"
        # companies 테이블은 제외 (다른 시스템에서도 사용할 수 있음)
    ]
    
    # 외래키 체크 비활성화
    db_manager.execute_query("SET FOREIGN_KEY_CHECKS = 0")
    
    success = True
    dropped_tables = []
    
    for table_name in tables_to_drop:
        try:
            # 테이블 존재 확인
            check_query = f"SHOW TABLES LIKE '{table_name}'"
            if db_manager.fetch_one(check_query):
                # 테이블 삭제
                drop_query = f"DROP TABLE {table_name}"
                if db_manager.execute_query(drop_query):
                    print(f"✅ {table_name} 테이블 삭제 완료")
                    dropped_tables.append(table_name)
                else:
                    print(f"❌ {table_name} 테이블 삭제 실패")
                    success = False
            else:
                print(f"⏭️  {table_name} 테이블이 존재하지 않음")
                
        except Exception as e:
            print(f"❌ {table_name} 테이블 삭제 중 오류: {e}")
            success = False
    
    # 외래키 체크 재활성화
    db_manager.execute_query("SET FOREIGN_KEY_CHECKS = 1")
    
    if success:
        print(f"🎉 총 {len(dropped_tables)}개 테이블 삭제 완료!")
        print(f"삭제된 테이블: {', '.join(dropped_tables)}")
    else:
        print("⚠️  일부 테이블 삭제에서 오류가 발생했습니다.")
    
    return success

def verify_clean_state(db_manager):
    """테이블이 깔끔하게 삭제되었는지 확인"""
    print("\n🔍 삭제 상태 확인 중...")
    
    tables_to_check = [
        "alerts", "predictions", "model_performance", "feature_data",
        "application_impact", "system_resources", "jvm_metrics", 
        "job_status", "configurations", "servers"
    ]
    
    remaining_tables = []
    
    for table_name in tables_to_check:
        check_query = f"SHOW TABLES LIKE '{table_name}'"
        if db_manager.fetch_one(check_query):
            remaining_tables.append(table_name)
    
    if remaining_tables:
        print(f"⚠️  남아있는 테이블: {', '.join(remaining_tables)}")
        return False
    else:
        print("✅ 모든 테이블이 깔끔하게 삭제되었습니다!")
        return True

def clean_and_setup(company_domain=None, device_id=None, force=False):
    """전체 테이블 삭제 후 새로 생성"""
    print("=" * 60)
    print("🧹 JVM 메트릭 시스템 클린 셋업 시작")
    print("=" * 60)
    
    db_manager = DatabaseManager()
    
    try:
        # 1. 기존 테이블 삭제
        print("\n1단계: 기존 테이블 삭제")
        if not drop_all_tables(db_manager, confirm=force):
            print("❌ 테이블 삭제 실패 또는 취소")
            return False
        
        # 2. 삭제 상태 확인
        print("\n2단계: 삭제 상태 확인")
        if not verify_clean_state(db_manager):
            print("❌ 일부 테이블이 남아있습니다.")
            return False
        
        # 3. 새로운 설정으로 전체 셋업
        print("\n3단계: 새로운 스키마로 전체 셋업")
        print("-" * 40)
        
        db_manager.close()  # 기존 연결 종료
        
        # setup_all 함수 실행
        if setup_all(company_domain, device_id):
            print("\n🎉 클린 셋업이 성공적으로 완료되었습니다!")
            print("=" * 60)
            return True
        else:
            print("\n❌ 새로운 셋업 중 오류가 발생했습니다.")
            return False
            
    except Exception as e:
        print(f"\n❌ 클린 셋업 중 오류 발생: {e}")
        import traceback
        print(traceback.format_exc())
        return False
    
    finally:
        if hasattr(db_manager, 'connection') and db_manager.connection:
            db_manager.close()

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='JVM 메트릭 시스템 클린 셋업')
    parser.add_argument('--company', help='회사 도메인 (지정하지 않으면 자동 감지)')
    parser.add_argument('--device', help='디바이스 ID (지정하지 않으면 자동 감지)')
    parser.add_argument('--auto', action='store_true', help='자동 감지 사용')
    parser.add_argument('--force', action='store_true', help='확인 없이 강제 실행')
    
    args = parser.parse_args()
    
    company_domain = None if args.auto else args.company
    device_id = None if args.auto else args.device
    
    print(f"🎯 타겟 설정:")
    print(f"   회사 도메인: {company_domain or '자동 감지'}")
    print(f"   디바이스 ID: {device_id or '자동 감지'}")
    print(f"   강제 실행: {'예' if args.force else '아니오'}")
    
    if clean_and_setup(company_domain, device_id, args.force):
        print("\n✨ 모든 작업이 성공적으로 완료되었습니다!")
        print("\n📋 다음 단계:")
        print("   1. python tests/test_field_migration.py  # 테스트 실행")
        print("   2. python scripts/collector.py --mode initial  # 초기 데이터 수집")
        print("   3. python scripts/train.py --mode train-all  # 모델 학습")
    else:
        print("\n💥 작업 중 오류가 발생했습니다. 로그를 확인해주세요.")

if __name__ == "__main__":
    main()