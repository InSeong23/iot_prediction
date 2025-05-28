import sys
import os

# 현재 스크립트의 상위 디렉토리를 시스템 경로에 추가 (임포트 전에 먼저 실행)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.db import DatabaseManager
import uuid
import argparse

def generate_test_data(company_domain, server_id=None, device_id=None, days=3, start_date=None):
    """테스트 데이터 생성 메인 함수"""
    print(f"'{company_domain}' 회사, 서버 ID {server_id}, 디바이스 ID {device_id}에 대한 테스트 데이터 생성 시작")
    
    # 시작 날짜 설정
    if start_date:
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00').replace('T', ' '))
        start_time = start_date
    else:
        start_time = datetime.now() - timedelta(days=days)
    
    end_time = start_time + timedelta(days=days)
    print(f"데이터 기간: {start_time} ~ {end_time}")
    
    # 서버 ID 확인 또는 생성
    db_manager = DatabaseManager()
    
    # device_id가 제공된 경우 우선 처리
    if device_id and not server_id:
        print(f"디바이스 ID '{device_id}'로 기존 서버 조회 중...")
        server_id = db_manager.get_server_by_device_id(company_domain, device_id)
        if server_id:
            print(f"기존 디바이스 서버 발견: 서버 ID {server_id}, 디바이스 ID {device_id}")
        else:
            print(f"디바이스 ID '{device_id}'에 해당하는 서버를 찾을 수 없음. 새로 생성합니다.")
            # 서버 자동 등록
            from scripts.setup import insert_server
            server_id = insert_server(db_manager, company_domain, device_id, f"Auto {device_id}", "auto")
            if server_id:
                print(f"새 디바이스 서버 생성: 서버 ID {server_id}, 디바이스 ID {device_id}")
            else:
                print(f"디바이스 '{device_id}' 서버 생성 실패")
                return False
    
    # server_id가 여전히 없으면 기본 테스트 서버 생성 또는 가져오기
    elif not server_id:
        print("기본 테스트 서버 확인 중...")
        # 기본 테스트 서버가 있는지 확인
        test_server_query = f"""
        SELECT {db_manager.server_id_field} FROM {db_manager.server_table}
        WHERE {db_manager.server_domain_field} = %s AND {db_manager.server_ip_field} = 'test-default'
        LIMIT 1
        """
        result = db_manager.fetch_one(test_server_query, (company_domain,))
        
        if result:
            server_id = result[0]
            print(f"기존 테스트 서버 사용: 서버 ID {server_id}")
        else:
            # 새 테스트 서버 생성
            from scripts.setup import insert_server
            server_id = insert_server(db_manager, company_domain, "test-default")
            if server_id:
                print(f"새 테스트 서버 생성: 서버 ID {server_id}")
            else:
                print("테스트 서버 생성 실패")
                return False
    
    # 시스템 리소스 데이터 생성
    sys_result = generate_system_resources(company_domain, server_id, device_id, start_time, end_time)
    
    # JVM 메트릭 데이터 생성
    jvm_result = generate_jvm_metrics(company_domain, server_id, device_id, start_time, end_time)
    
    # 결과 출력
    if sys_result and jvm_result:
        print("테스트 데이터 생성 성공!")
    else:
        print("테스트 데이터 생성 중 오류 발생")
    
    return sys_result and jvm_result

def generate_system_resources(company_domain, server_id, device_id, start_time, end_time):
    """시스템 리소스 테스트 데이터 생성"""
    db = DatabaseManager()
    batch_id = str(uuid.uuid4())
    
    # 5분 간격 타임스탬프 생성
    timestamps = pd.date_range(start=start_time, end=end_time, freq='5min')
    
    # 리소스 유형 및 측정 항목
    resources = [
        ('cpu', 'usage_user'),
        ('mem', 'used_percent'),
        ('disk', 'used_percent')
    ]
    
    batch_data = []
    
    # 데이터 생성
    for ts in timestamps:
        for res_type, measurement in resources:
            # 기본 트렌드 + 노이즈 + 일별 패턴
            hour_factor = np.sin(ts.hour / 24 * 2 * np.pi) * 10
            
            if res_type == 'cpu':
                # CPU: 20% ~ 80% 사이 변동
                base = 40 + hour_factor
                noise = np.random.normal(0, 5)
            elif res_type == 'mem':
                # 메모리: 50% ~ 75% 사이 변동
                base = 60 + hour_factor/2
                noise = np.random.normal(0, 3)
            else:
                # 디스크: 70% ~ 85% 사이 완만한 증가
                days_factor = (ts - start_time).total_seconds() / (end_time - start_time).total_seconds() * 10
                base = 70 + days_factor + hour_factor/4
                noise = np.random.normal(0, 1)
            
            value = min(max(base + noise, 5), 98)  # 5% ~ 98% 범위로 제한
            
            # device_id가 없으면 기본값 생성
            actual_device_id = device_id if device_id else f"test_device_{server_id}"
            
            batch_data.append((
                company_domain,
                server_id,
                ts,
                res_type,
                measurement,
                value,
                actual_device_id,
                batch_id
            ))
    
    # 데이터 삽입 - 동적 필드명 사용
    query = f"""
    INSERT INTO system_resources 
    (company_domain, {db.server_id_field}, time, resource_type, measurement, value, device_id, batch_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    result = db.execute_query(query, batch_data, many=True)
    print(f"시스템 리소스 데이터 {len(batch_data)}개 생성 완료")
    
    return result

def generate_jvm_metrics(company_domain, server_id, device_id, start_time, end_time):
    """JVM 메트릭 테스트 데이터 생성"""
    db = DatabaseManager()
    batch_id = str(uuid.uuid4())
    
    # 5분 간격 타임스탬프 생성
    timestamps = pd.date_range(start=start_time, end=end_time, freq='5min')
    
    # 애플리케이션 목록
    applications = [
        'javame-gateway', 'javame-member', 'javame-frontend', 
        'javame-environment-api', 'javame-auth'
    ]
    
    # 메트릭 유형
    metrics = [
        'cpu_utilization_percent',
        'gc_g1_young_generation_count',
        'memory_old_gen_used_bytes',
        'memory_total_heap_used_bytes',
        'process_open_file_descriptors_count',
        'thread_active_count'
    ]
    
    batch_data = []
    
    # 데이터 생성
    for ts in timestamps:
        for app in applications:
            for metric in metrics:
                # 시간대별 변동 요소
                hour_factor = np.sin(ts.hour / 24 * 2 * np.pi)
                
                if metric == 'cpu_utilization_percent':
                    # CPU 사용률: 5% ~ 70%
                    base = 30 + hour_factor * 20
                    noise = np.random.normal(0, 5)
                    value = min(max(base + noise, 5), 90)
                    
                elif metric == 'memory_total_heap_used_bytes':
                    # 힙 메모리: 500MB ~ 2GB (바이트 단위)
                    base = 1_000_000_000 + hour_factor * 500_000_000
                    noise = np.random.normal(0, 100_000_000)
                    value = max(base + noise, 100_000_000)
                    
                elif metric == 'memory_old_gen_used_bytes':
                    # Old Gen 메모리: 300MB ~ 1.5GB
                    base = 700_000_000 + hour_factor * 400_000_000
                    noise = np.random.normal(0, 50_000_000)
                    value = max(base + noise, 50_000_000)
                    
                elif metric == 'gc_g1_young_generation_count':
                    # GC 횟수: 누적값 (증가)
                    days_passed = (ts - start_time).total_seconds() / (end_time - start_time).total_seconds() * 3
                    base = days_passed * 1000 + hour_factor * 20
                    noise = np.random.normal(0, 5)
                    value = max(base + noise, 0)
                    
                elif metric == 'thread_active_count':
                    # 스레드 수: 50 ~ 200
                    base = 100 + hour_factor * 50
                    noise = np.random.normal(0, 10)
                    value = max(base + noise, 10)
                    
                else:  # process_open_file_descriptors_count
                    # 파일 디스크립터: 200 ~ 800
                    base = 400 + hour_factor * 200
                    noise = np.random.normal(0, 20)
                    value = max(base + noise, 50)
                
                # device_id가 없으면 기본값 생성
                actual_device_id = device_id if device_id else f"test_device_{server_id}"
                
                batch_data.append((
                    company_domain,
                    server_id,
                    ts,
                    app,
                    metric,
                    value,
                    actual_device_id,
                    batch_id
                ))
    
    # 데이터 삽입 - 동적 필드명 사용
    query = f"""
    INSERT INTO jvm_metrics 
    (company_domain, {db.server_id_field}, time, application, metric_type, value, device_id, batch_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    result = db.execute_query(query, batch_data, many=True)
    print(f"JVM 메트릭 데이터 {len(batch_data)}개 생성 완료")
    
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='테스트 데이터 생성')
    parser.add_argument('--company', default='javame', help='회사 도메인')
    parser.add_argument('--server', type=int, help='서버 ID')
    parser.add_argument('--device', help='디바이스 ID')
    parser.add_argument('--days', default=3, type=int, help='생성할 데이터 일수')
    parser.add_argument('--start', help='시작 날짜 (ISO 형식: YYYY-MM-DDTHH:MM:SS)')
    
    args = parser.parse_args()
    
    generate_test_data(args.company, args.server, args.device, args.days, args.start)