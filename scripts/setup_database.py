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
            company_domain VARCHAR(50) NOT NULL,  -- 길이 변경
            server_iphost VARCHAR(50) DEFAULT NULL,  -- 길이 변경
            influx_database VARCHAR(100) NOT NULL,
            influx_retention VARCHAR(50) DEFAULT '3d',
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            INDEX idx_domain_host (company_domain, server_iphost)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_servers)
        
        # model_metadata 테이블 생성 
        create_model_metadata = """
        CREATE TABLE IF NOT EXISTS model_metadata (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            model_path VARCHAR(255) NOT NULL,
            model_version VARCHAR(50) NOT NULL,
            training_start DATETIME NOT NULL,
            training_end DATETIME NOT NULL,
            metrics JSON,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_model_lookup (company_domain, server_no, is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        db.execute_query(create_model_metadata)

        # predictions 테이블 수정
        create_predictions = """
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) NOT NULL,
            server_no INT NOT NULL,
            prediction_time DATETIME NOT NULL,
            target_time DATETIME NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            predicted_value DOUBLE NOT NULL,
            model_version VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY (server_no) REFERENCES servers(server_no),
            INDEX idx_prediction (company_domain, server_no, target_time)
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