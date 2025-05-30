#!/usr/bin/env python
"""
간소화된 데이터베이스 설정 - 스트리밍 아키텍처용
필수 테이블만 생성, 시계열 데이터는 InfluxDB에서 직접 처리
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import DatabaseManager
from core.logger import logger

def create_streamlined_tables():
    """스트리밍 아키텍처용 간소화된 테이블 생성"""
    db = DatabaseManager()
    
    try:
        # 기본 회사 테이블 확인
        check_query = "SHOW TABLES LIKE 'companies'"
        if not db.fetch_one(check_query):
            logger.error("companies 테이블이 없습니다. 먼저 생성해주세요.")
            return False
        
        # 서버 테이블 (회사별 디바이스 관리)
        create_servers = """
        CREATE TABLE IF NOT EXISTS servers (
            server_no INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(30) NOT NULL,
            server_iphost VARCHAR(20) DEFAULT NULL COMMENT '디바이스 ID',
            server_name VARCHAR(100) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            INDEX idx_domain_device (company_domain, server_iphost),
            INDEX idx_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='서버/디바이스 정보 관리'
        """
        db.execute_query(create_servers)
        logger.info("servers 테이블 생성/확인 완료")
        
        # 설정 테이블 (회사별/서버별 설정 관리)
        create_configurations = """
        CREATE TABLE IF NOT EXISTS configurations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NULL,
            config_type VARCHAR(50) NOT NULL COMMENT '설정 유형 (aggregation, excluded_devices 등)',
            config_key VARCHAR(100) NOT NULL COMMENT '설정 키',
            config_value TEXT NOT NULL COMMENT '설정 값 (JSON 가능)',
            is_active BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            UNIQUE KEY uk_config (company_domain, server_no, config_type, config_key),
            INDEX idx_type_key (config_type, config_key),
            INDEX idx_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='시스템 설정 정보'
        """
        db.execute_query(create_configurations)
        logger.info("configurations 테이블 생성/확인 완료")
        
        # 모델 성능 테이블 (학습된 모델 성능 추적)
        create_model_performance = """
        CREATE TABLE IF NOT EXISTS model_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            application VARCHAR(100) NOT NULL COMMENT '애플리케이션명 (system은 시스템 모델)',
            resource_type VARCHAR(50) NOT NULL COMMENT 'cpu, mem, disk',
            model_type VARCHAR(50) NOT NULL COMMENT 'random_forest, gradient_boosting 등',
            mae DOUBLE NOT NULL COMMENT '평균 절대 오차',
            rmse DOUBLE NOT NULL COMMENT '평균 제곱근 오차',
            r2_score DOUBLE NOT NULL COMMENT 'R² 점수',
            feature_importance JSON COMMENT '특성 중요도',
            trained_at DATETIME NOT NULL COMMENT '학습 시간',
            version VARCHAR(50) NOT NULL COMMENT '모델 버전',
            device_id VARCHAR(100) DEFAULT '' COMMENT '디바이스 ID',
            cache_path VARCHAR(500) DEFAULT NULL COMMENT '모델 파일 경로',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_model (company_domain, server_no, application, resource_type),
            INDEX idx_device_id (device_id),
            INDEX idx_trained_at (trained_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='모델 성능 및 메타데이터'
        """
        db.execute_query(create_model_performance)
        logger.info("model_performance 테이블 생성/확인 완료")
        
        # 예측 결과 테이블 (실제 예측 결과만 저장)
        create_predictions = """
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            prediction_time DATETIME NOT NULL COMMENT '예측 수행 시간',
            target_time DATETIME NOT NULL COMMENT '예측 대상 시간',
            resource_type VARCHAR(50) NOT NULL COMMENT 'cpu, mem, disk',
            predicted_value DOUBLE NOT NULL COMMENT '예측값',
            actual_value DOUBLE NULL COMMENT '실제값 (나중에 업데이트)',
            error DOUBLE NULL COMMENT '예측 오차',
            model_version VARCHAR(100) NOT NULL COMMENT '사용된 모델 버전',
            confidence_score DOUBLE DEFAULT NULL COMMENT '예측 신뢰도',
            batch_id VARCHAR(100) DEFAULT NULL COMMENT '배치 ID',
            device_id VARCHAR(100) DEFAULT '' COMMENT '디바이스 ID',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_prediction (company_domain, server_no, resource_type, target_time),
            INDEX idx_device_id (device_id),
            INDEX idx_accuracy (target_time, actual_value),
            INDEX idx_batch (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='예측 결과 저장'
        """
        db.execute_query(create_predictions)
        logger.info("predictions 테이블 생성/확인 완료")
        
        # 알림 테이블 (임계값 도달 알림)
        create_alerts = """
        CREATE TABLE IF NOT EXISTS alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            resource_type VARCHAR(50) NOT NULL COMMENT 'cpu, mem, disk',
            threshold DOUBLE NOT NULL COMMENT '설정된 임계값',
            crossing_time DATETIME NOT NULL COMMENT '임계값 도달 예상 시간',
            time_to_threshold DOUBLE NOT NULL COMMENT '임계값까지 남은 시간(시)',
            current_value DOUBLE NOT NULL COMMENT '현재 값',
            predicted_value DOUBLE NOT NULL COMMENT '예측값',
            is_notified BOOLEAN DEFAULT FALSE COMMENT '알림 발송 여부',
            notified_at DATETIME NULL COMMENT '알림 발송 시간',
            alert_level VARCHAR(20) DEFAULT 'warning' COMMENT 'info, warning, critical',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            batch_id VARCHAR(100) DEFAULT NULL,
            device_id VARCHAR(100) DEFAULT '' COMMENT '디바이스 ID',
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_alert (company_domain, server_no, resource_type, created_at),
            INDEX idx_device_id (device_id),
            INDEX idx_notification (is_notified, created_at),
            INDEX idx_level (alert_level)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='시스템 알림 관리'
        """
        db.execute_query(create_alerts)
        logger.info("alerts 테이블 생성/확인 완료")
        
        # 작업 로그 테이블 (스케줄러 작업 추적)
        create_job_logs = """
        CREATE TABLE IF NOT EXISTS job_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NULL COMMENT 'NULL이면 전체 회사 작업',
            job_type VARCHAR(50) NOT NULL COMMENT 'data_collection, model_training, prediction 등',
            job_status VARCHAR(20) NOT NULL COMMENT 'running, completed, failed',
            start_time DATETIME NOT NULL,
            end_time DATETIME NULL,
            duration_seconds INT NULL COMMENT '실행 시간(초)',
            records_processed INT DEFAULT 0 COMMENT '처리된 레코드 수',
            cache_files_created INT DEFAULT 0 COMMENT '생성된 캐시 파일 수',
            error_message TEXT NULL COMMENT '오류 메시지',
            details JSON NULL COMMENT '상세 정보',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_job (company_domain, server_no, job_type, start_time),
            INDEX idx_status (job_status, start_time),
            INDEX idx_type (job_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='작업 실행 로그'
        """
        db.execute_query(create_job_logs)
        logger.info("job_logs 테이블 생성/확인 완료")
        
        # 캐시 메타데이터 테이블 (파일 캐시 관리)
        create_cache_metadata = """
        CREATE TABLE IF NOT EXISTS cache_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            cache_type VARCHAR(50) NOT NULL COMMENT 'jvm_data, sys_data, impacts, features',
            cache_key VARCHAR(200) NOT NULL COMMENT '캐시 키 (시간 범위 등)',
            file_path VARCHAR(500) NOT NULL COMMENT '파일 경로',
            file_size_bytes BIGINT DEFAULT 0 COMMENT '파일 크기',
            record_count INT DEFAULT 0 COMMENT '레코드 수',
            start_time DATETIME NOT NULL COMMENT '데이터 시작 시간',
            end_time DATETIME NOT NULL COMMENT '데이터 종료 시간',
            expires_at DATETIME NOT NULL COMMENT '만료 시간',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            device_id VARCHAR(100) DEFAULT '' COMMENT '디바이스 ID',
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            UNIQUE KEY uk_cache (company_domain, server_no, cache_type, cache_key),
            INDEX idx_expires (expires_at),
            INDEX idx_device (device_id),
            INDEX idx_type_time (cache_type, start_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='파일 캐시 메타데이터'
        """
        db.execute_query(create_cache_metadata)
        logger.info("cache_metadata 테이블 생성/확인 완료")
        
        logger.info("모든 스트리밍 아키텍처 테이블 생성 완료")
        return True
        
    except Exception as e:
        logger.error(f"테이블 생성 중 오류: {e}")
        return False
    finally:
        db.close()

def insert_default_streaming_configs():
    """스트리밍 아키텍처용 기본 설정 삽입"""
    db = DatabaseManager()
    
    try:
        # 1. 기존 회사 중 첫 번째 회사를 기본 설정 대상으로 선택
        existing_companies = db.fetch_all("SELECT company_domain FROM companies WHERE is_active = 1 ORDER BY registered_at LIMIT 5")
        
        if not existing_companies:
            logger.error("활성화된 회사가 없어 기본 설정을 삽입할 수 없습니다")
            return False
        
        # 'javame' 회사가 있으면 우선 사용, 없으면 첫 번째 회사 사용
        target_company = None
        for company_row in existing_companies:
            company_domain = company_row[0]
            if company_domain == 'javame':
                target_company = company_domain
                break
        
        if not target_company:
            target_company = existing_companies[0][0]
        
        logger.info(f"기본 설정 삽입 대상 회사: '{target_company}'")
        
        # 2. 기본 설정 데이터
        global_configs = [
            # 시간 정렬 설정 (가장 중요)
            ('time_alignment', 'time_alignment_minutes', '1'),
            ('time_alignment', 'aggregation_window_seconds', '30'),
            ('time_alignment', 'time_tolerance_minutes', '2'),
            ('time_alignment', 'max_prediction_age_hours', '48'),
            
            # 캐시 설정
            ('cache', 'default_ttl_hours', '6'),
            ('cache', 'max_file_age_hours', '48'),
            ('cache', 'cleanup_interval_hours', '12'),
            
            # 데이터 수집 설정
            ('collection', 'training_data_days', '3'),
            ('collection', 'latest_data_minutes', '30'),
            ('collection', 'influxdb_timeout_seconds', '60'),
            
            # 집계 설정 (CPU 사용률 계산 포함)
            ('aggregation', 'cpu', '{"method": "max", "reason": "피크 사용률이 중요", "calculation": "100 - usage_idle"}'),
            ('aggregation', 'mem', '{"method": "last", "reason": "현재 상태가 중요"}'),
            ('aggregation', 'disk', '{"method": "last", "reason": "누적 사용량"}'),
            
            # 제외 목록
            ('excluded', 'gateways', 'diskio,net,sensors,swap,system,http,unknown_service,입구,jvm'),
            ('excluded', 'devices', 'nhnacademy-Inspiron-14-5420,24e124743d011875,24e124136d151368'),
            
            # 모델 설정
            ('model', 'retrain_threshold_hours', '24'),
            ('model', 'min_training_records', '100'),
            ('model', 'feature_cache_enabled', 'true'),
            
            # 알림 설정
            ('alert', 'cpu_threshold', '80.0'),
            ('alert', 'memory_threshold', '80.0'),
            ('alert', 'disk_threshold', '85.0'),
            ('alert', 'notification_cooldown_minutes', '60')
        ]
        
        # 3. 기존 설정 확인 후 필요시에만 삽입
        check_query = """
        SELECT COUNT(*) FROM configurations 
        WHERE company_domain = %s AND config_type = %s AND config_key = %s
        """
        
        insert_query = """
        INSERT INTO configurations 
        (company_domain, server_no, config_type, config_key, config_value, is_active)
        VALUES (%s, NULL, %s, %s, %s, TRUE)
        """
        
        inserted_count = 0
        
        for config_type, config_key, config_value in global_configs:
            # 이미 존재하는지 확인
            existing = db.fetch_one(check_query, (target_company, config_type, config_key))
            
            if not existing or existing[0] == 0:
                # 존재하지 않으면 삽입
                if db.execute_query(insert_query, (target_company, config_type, config_key, config_value)):
                    inserted_count += 1
                    logger.debug(f"설정 삽입: {config_type}.{config_key} = {config_value}")
                else:
                    logger.warning(f"설정 삽입 실패: {config_type}.{config_key}")
            else:
                logger.debug(f"설정 이미 존재: {config_type}.{config_key}")
        
        logger.info(f"스트리밍 아키텍처 기본 설정 처리 완료: {inserted_count}개 신규 삽입 (대상: {target_company})")
        return True
        
    except Exception as e:
        logger.error(f"기본 설정 삽입 오류: {e}")
        return False
    finally:
        db.close()

def cleanup_legacy_tables():
    """기존 불필요한 테이블 정리 (선택적)"""
    db = DatabaseManager()
    
    # 주의: 실제 운영 환경에서는 데이터 백업 후 실행
    legacy_tables = [
        'jvm_metrics',
        'system_resources', 
        'application_impact',
        'feature_data'
    ]
    
    try:
        for table in legacy_tables:
            # 테이블 존재 확인
            check_query = f"SHOW TABLES LIKE '{table}'"
            if db.fetch_one(check_query):
                logger.warning(f"기존 테이블 발견: {table}")
                logger.warning(f"데이터 백업 후 'DROP TABLE {table}' 실행을 고려하세요")
                
                # 자동 삭제는 하지 않음 (안전을 위해)
                # drop_query = f"DROP TABLE IF EXISTS {table}"
                # db.execute_query(drop_query)
        
        logger.info("기존 테이블 정리 확인 완료")
        return True
        
    except Exception as e:
        logger.error(f"기존 테이블 정리 중 오류: {e}")
        return False
    finally:
        db.close()

def validate_streaming_setup():
    """스트리밍 아키텍처 설정 검증"""
    db = DatabaseManager()
    
    try:
        required_tables = [
            'companies', 'servers', 'configurations', 
            'model_performance', 'predictions', 'alerts',
            'job_logs', 'cache_metadata'
        ]
        
        missing_tables = []
        for table in required_tables:
            check_query = f"SHOW TABLES LIKE '{table}'"
            if not db.fetch_one(check_query):
                missing_tables.append(table)
        
        if missing_tables:
            logger.error(f"필수 테이블이 없습니다: {missing_tables}")
            return False
        
        # 설정 확인 - 실제 회사 도메인에서 확인
        config_query = """
        SELECT COUNT(*) FROM configurations 
        WHERE config_type IN ('time_alignment', 'cache', 'aggregation', 'excluded', 'model', 'alert')
        """
        config_result = db.fetch_one(config_query)
        config_count = config_result[0] if config_result else 0
        
        logger.info(f"발견된 기본 설정: {config_count}개")
        
        # 최소 5개 이상의 설정이 있으면 통과
        if config_count < 5:
            logger.warning(f"기본 설정이 부족합니다 ({config_count}개). 최소 5개 필요.")
            
            # 설정 유형별 상세 확인
            detail_query = """
            SELECT config_type, COUNT(*) 
            FROM configurations 
            GROUP BY config_type
            """
            details = db.fetch_all(detail_query)
            
            if details:
                logger.info("현재 설정 유형별 개수:")
                for config_type, count in details:
                    logger.info(f"  {config_type}: {count}개")
            
            return False
        
        logger.info("스트리밍 아키텍처 설정 검증 완료")
        return True
        
    except Exception as e:
        logger.error(f"설정 검증 중 오류: {e}")
        return False
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("스트리밍 아키텍처 데이터베이스 설정 시작")
    
    success = True
    
    # 1. 테이블 생성
    if not create_streamlined_tables():
        success = False
    
    # 2. 기본 설정 삽입
    if success and not insert_default_streaming_configs():
        success = False
    
    # 3. 기존 테이블 정리 확인
    if success:
        cleanup_legacy_tables()
    
    # 4. 설정 검증
    if success and not validate_streaming_setup():
        success = False
    
    if success:
        logger.info("스트리밍 아키텍처 데이터베이스 설정 완료")
        print("✅ 스트리밍 아키텍처로 성공적으로 전환되었습니다!")
        print("")
        print("📊 변경사항:")
        print("  - jvm_metrics, system_resources → InfluxDB 직접 조회")
        print("  - application_impact, feature_data → 파일 캐싱")
        print("  - 필수 테이블만 유지하여 DB 부하 최소화")
        print("")
        print("🗂️ 캐시 디렉토리:")
        print("  - cache/{company}/{device}/")
        print("    ├── raw 데이터 캐시 (jvm_*.pkl, sys_*.pkl)")
        print("    ├── impacts/ (영향도 캐시)")
        print("    └── features/ (특성 캐시)")
    else:
        logger.error("스트리밍 아키텍처 데이터베이스 설정 실패")
        sys.exit(1)