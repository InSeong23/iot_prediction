#!/usr/bin/env python
"""
데이터베이스 초기 설정 스크립트
테이블 생성 및 기본 설정만 수행
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import DatabaseManager
from core.logger import logger

def create_tables_if_not_exists():
    """필요한 테이블이 없으면 생성"""
    db = DatabaseManager()
    
    try:
        # companies 테이블 확인
        check_query = "SHOW TABLES LIKE 'companies'"
        if not db.fetch_one(check_query):
            logger.info("companies 테이블이 없습니다. 다른 시스템에서 생성되어야 합니다.")
            return False
        
        # servers 테이블 생성
        create_servers = """
        CREATE TABLE IF NOT EXISTS servers (
            server_no INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(30) NOT NULL,
            server_iphost VARCHAR(20) DEFAULT NULL,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            INDEX idx_domain_host (company_domain, server_iphost)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_servers)
        
        # system_resources 테이블 생성
        create_system_resources = """
        CREATE TABLE IF NOT EXISTS system_resources (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            time DATETIME NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            measurement VARCHAR(100) NOT NULL,
            value DOUBLE NOT NULL,
            device_id VARCHAR(100) NOT NULL DEFAULT '',
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_time_resource (time, resource_type),
            INDEX idx_device_time (device_id, time),
            INDEX idx_lookup (company_domain, server_no, resource_type, time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_system_resources)
        
        # jvm_metrics 테이블 생성
        create_jvm_metrics = """
        CREATE TABLE IF NOT EXISTS jvm_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            time DATETIME NOT NULL,
            application VARCHAR(100) NOT NULL,
            metric_type VARCHAR(100) NOT NULL,
            value DOUBLE NOT NULL,
            device_id VARCHAR(100) NOT NULL DEFAULT '',
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_time_app (time, application),
            INDEX idx_device_time (device_id, time),
            INDEX idx_lookup (company_domain, server_no, application, time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_jvm_metrics)
        
        # application_impact 테이블 생성
        create_application_impact = """
        CREATE TABLE IF NOT EXISTS application_impact (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            time DATETIME NOT NULL,
            application VARCHAR(100) NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            impact_score DOUBLE NOT NULL,
            calculation_method VARCHAR(50) NOT NULL,
            device_id VARCHAR(100) DEFAULT '',
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_time_app_resource (time, application, resource_type),
            INDEX idx_device_time (device_id, time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_application_impact)
        
        # predictions 테이블 생성
        create_predictions = """
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            prediction_time DATETIME NOT NULL,
            target_time DATETIME NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            predicted_value DOUBLE NOT NULL,
            actual_value DOUBLE,
            error DOUBLE,
            model_version VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100),
            device_id VARCHAR(100) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_prediction (company_domain, server_no, resource_type, target_time),
            INDEX idx_device_id (device_id),
            INDEX idx_accuracy (target_time, actual_value)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_predictions)
        
        # configurations 테이블 생성
        create_configurations = """
        CREATE TABLE IF NOT EXISTS configurations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NULL,
            config_type VARCHAR(50) NOT NULL,
            config_key VARCHAR(100) NOT NULL,
            config_value TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            UNIQUE KEY uk_config (company_domain, server_no, config_type, config_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_configurations)
        
        # 기타 필요한 테이블들
        other_tables = [
            """
            CREATE TABLE IF NOT EXISTS feature_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                company_domain VARCHAR(50) NOT NULL,
                server_no INT NOT NULL,
                time DATETIME NOT NULL,
                application VARCHAR(100) NOT NULL,
                feature_name VARCHAR(100) NOT NULL,
                value DOUBLE NOT NULL,
                window_size INT NOT NULL,
                device_id VARCHAR(100) DEFAULT '',
                batch_id VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_time_app_feature (time, application, feature_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS model_performance (
                id INT AUTO_INCREMENT PRIMARY KEY,
                company_domain VARCHAR(50) NOT NULL,
                server_no INT NOT NULL,
                application VARCHAR(100) NOT NULL,
                resource_type VARCHAR(50) NOT NULL,
                model_type VARCHAR(50) NOT NULL,
                mae DOUBLE NOT NULL,
                rmse DOUBLE NOT NULL,
                r2_score DOUBLE NOT NULL,
                feature_importance JSON,
                trained_at DATETIME NOT NULL,
                version VARCHAR(50) NOT NULL,
                device_id VARCHAR(100) DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_model (company_domain, server_no, application, resource_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                company_domain VARCHAR(50) NOT NULL,
                server_no INT NOT NULL,
                resource_type VARCHAR(50) NOT NULL,
                threshold DOUBLE NOT NULL,
                crossing_time DATETIME NOT NULL,
                time_to_threshold DOUBLE NOT NULL,
                current_value DOUBLE NOT NULL,
                predicted_value DOUBLE NOT NULL,
                is_notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                batch_id VARCHAR(100),
                device_id VARCHAR(100) DEFAULT '',
                INDEX idx_alert (company_domain, server_no, resource_type, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ]
        
        for create_query in other_tables:
            db.execute_query(create_query)
        
        logger.info("모든 테이블 생성 완료")
        return True
        
    except Exception as e:
        logger.error(f"테이블 생성 중 오류: {e}")
        return False
    finally:
        db.close()

def insert_default_configurations():
    """기본 설정 삽입"""
    db = DatabaseManager()
    
    try:
        # 기본 집계 설정
        default_configs = [
            ('aggregation', 'cpu', '{"method": "max", "reason": "피크 사용률이 중요"}'),
            ('aggregation', 'mem', '{"method": "last", "reason": "현재 상태가 중요"}'),
            ('aggregation', 'disk', '{"method": "last", "reason": "누적 사용량"}'),
            ('excluded_gateways', 'list', 'diskio,net,sensors,swap,system,http,unknown_service,입구,jvm'),
            ('excluded_devices', 'list', 'nhnacademy-Inspiron-14-5420,24e124743d011875,24e124136d151368')
        ]
        
        for config_type, config_key, config_value in default_configs:
            query = """
            INSERT IGNORE INTO configurations 
            (company_domain, server_no, config_type, config_key, config_value, is_active)
            VALUES ('*', NULL, %s, %s, %s, TRUE)
            """
            db.execute_query(query, (config_type, config_key, config_value))
        
        logger.info("기본 설정 삽입 완료")
        return True
        
    except Exception as e:
        logger.error(f"기본 설정 삽입 오류: {e}")
        return False
    finally:
        db.close()

if __name__ == "__main__":
    logger.info("데이터베이스 초기 설정 시작")
    
    if create_tables_if_not_exists():
        insert_default_configurations()
        logger.info("데이터베이스 초기 설정 완료")
    else:
        logger.error("데이터베이스 초기 설정 실패")
        sys.exit(1)