"""
JVM 메트릭 기반 시스템 리소스 예측 - 초기 설정 스크립트
- 데이터베이스 테이블 생성
- 기본 회사 및 서버 설정
- 설정 정보 등록
"""
import sys
import os
import argparse
from datetime import datetime

# 현재 스크립트의 상위 디렉토리를 시스템 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logger import logger
from core.db import DatabaseManager, DB_SCHEMA
from config.settings import COMPANY_DOMAIN, SERVER_CODE, COLLECTION

def create_tables(db_manager):
    """필요한 데이터베이스 테이블 생성 - 타입 호환성 수정"""
    logger.info("데이터베이스 테이블 생성 시작")
    
    # 회사 테이블 구조 확인
    check_query = """
    SELECT 
        COLUMN_NAME, 
        CHARACTER_SET_NAME, 
        COLLATION_NAME 
    FROM 
        INFORMATION_SCHEMA.COLUMNS 
    WHERE 
        TABLE_SCHEMA = DATABASE() AND 
        TABLE_NAME = 'companies' AND 
        COLUMN_NAME = 'company_domain'
    """
    
    result = db_manager.fetch_one(check_query)
    if result:
        column_name, charset, collation = result
        logger.info(f"companies 테이블의 company_domain 열 정보: {charset}, {collation}")
    else:
        charset = "utf8mb4"
        collation = "utf8mb4_unicode_ci"
        logger.warning(f"companies 테이블 정보를 가져올 수 없어 기본값을 사용합니다: {charset}, {collation}")
    
    # DB_SCHEMA에서 테이블 및 필드 이름 가져오기
    server_table = DB_SCHEMA.get("server_table", "servers")
    server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
    server_ip_field = DB_SCHEMA.get("server_ip_field", "server_iphost")
    server_domain_field = DB_SCHEMA.get("server_domain_field", "company_domain")
    
    # ⭐ 중요: 서버 테이블 타입을 INT로 고정
    ref_field_type = "INT"
    ref_nullable = "NOT NULL"
    ref_auto_increment = "AUTO_INCREMENT"
    
    logger.info(f"서버 ID 필드 타입: {ref_field_type} {ref_auto_increment}")
    
    # 테이블 정의 - INT 타입 사용
    tables = {
        # 서버 테이블 - INT AUTO_INCREMENT 사용
        server_table: f"""
        CREATE TABLE IF NOT EXISTS {server_table} (
            {server_id_field} {ref_field_type} {ref_auto_increment} PRIMARY KEY,
            {server_domain_field} VARCHAR(30) NOT NULL,
            {server_ip_field} VARCHAR(20) DEFAULT NULL,
            FOREIGN KEY ({server_domain_field}) REFERENCES companies(company_domain)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # JVM 메트릭 테이블
        "jvm_metrics": f"""
        CREATE TABLE IF NOT EXISTS jvm_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
            time DATETIME NOT NULL,
            application VARCHAR(100) NOT NULL,
            metric_type VARCHAR(100) NOT NULL,
            value DOUBLE NOT NULL,
            device_id VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_time_app (time, application),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 시스템 리소스 테이블
        "system_resources": f"""
        CREATE TABLE IF NOT EXISTS system_resources (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
            time DATETIME NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            measurement VARCHAR(100) NOT NULL,
            value DOUBLE NOT NULL,
            device_id VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_time_resource (time, resource_type),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 애플리케이션 영향도 테이블
        "application_impact": f"""
        CREATE TABLE IF NOT EXISTS application_impact (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
            time DATETIME NOT NULL,
            application VARCHAR(100) NOT NULL,
            resource_type VARCHAR(50) NOT NULL,
            impact_score DOUBLE NOT NULL,
            calculation_method VARCHAR(50) NOT NULL,
            device_id VARCHAR(100) DEFAULT '',
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_time_app_resource (time, application, resource_type),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 특성 데이터 테이블
        "feature_data": f"""
        CREATE TABLE IF NOT EXISTS feature_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
            time DATETIME NOT NULL,
            application VARCHAR(100) NOT NULL,
            feature_name VARCHAR(100) NOT NULL,
            value DOUBLE NOT NULL,
            window_size INT NOT NULL,
            device_id VARCHAR(100) DEFAULT '',
            batch_id VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_time_app_feature (time, application, feature_name),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 설정 테이블
        "configurations": f"""
        CREATE TABLE IF NOT EXISTS configurations (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NULL,
            config_type VARCHAR(50) NOT NULL,
            config_key VARCHAR(100) NOT NULL,
            config_value TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            UNIQUE KEY (company_domain, {server_id_field}, config_type, config_key)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 작업 상태 테이블
        "job_status": f"""
        CREATE TABLE IF NOT EXISTS job_status (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
            job_type VARCHAR(50) NOT NULL,
            start_time DATETIME NOT NULL,
            end_time DATETIME NULL,
            status VARCHAR(20) NOT NULL,
            last_processed_time DATETIME NOT NULL,
            details TEXT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_job_status (company_domain, {server_id_field}, job_type, status)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 모델 성능 테이블
        "model_performance": f"""
        CREATE TABLE IF NOT EXISTS model_performance (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
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
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_model (company_domain, {server_id_field}, application, resource_type),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 예측 결과 테이블
        "predictions": f"""
        CREATE TABLE IF NOT EXISTS predictions (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
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
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_prediction (company_domain, {server_id_field}, resource_type, target_time),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """,
        
        # 알림 테이블
        "alerts": f"""
        CREATE TABLE IF NOT EXISTS alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_domain VARCHAR(50) CHARACTER SET {charset} COLLATE {collation} NOT NULL,
            {server_id_field} {ref_field_type} NOT NULL,
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
            FOREIGN KEY (company_domain) REFERENCES companies(company_domain),
            FOREIGN KEY ({server_id_field}) REFERENCES {server_table}({server_id_field}),
            INDEX idx_alert (company_domain, {server_id_field}, resource_type, created_at),
            INDEX idx_device_id (device_id)
        ) ENGINE=InnoDB DEFAULT CHARSET={charset} COLLATE={collation};
        """
    }
    
    # 각 테이블 생성 실행
    success = True
    for table_name, query in tables.items():
        try:
            result = db_manager.execute_query(query)
            if result:
                logger.info(f"{table_name} 테이블 생성 완료")
            else:
                logger.error(f"{table_name} 테이블 생성 실패")
                success = False
        except Exception as e:
            logger.error(f"{table_name} 테이블 생성 중 오류 발생: {e}")
            success = False
    
    return success

def insert_company(db_manager, company_domain, company_name=None):
    """회사 정보 삽입 또는 확인"""
    logger.info(f"회사 정보 설정: {company_domain}")
    
    # 회사가 이미 존재하는지 확인
    check_query = "SELECT 1 FROM companies WHERE company_domain = %s"
    exists = db_manager.fetch_one(check_query, (company_domain,))
    
    if exists:
        logger.info(f"회사 '{company_domain}'가 이미 존재합니다.")
        return True
    
    # 회사 정보가 없으면 삽입
    if company_name is None:
        company_name = f"{company_domain.capitalize()} Company"
    
    insert_query = """
    INSERT INTO companies 
    (company_domain, company_name, company_email, company_mobile, company_address, is_active) 
    VALUES 
    (%s, %s, %s, %s, %s, %s)
    """
    
    params = (
        company_domain,                    # company_domain
        company_name,                      # company_name
        f"admin@{company_domain}.com",     # company_email
        '010-1234-5678',                   # company_mobile
        '서울시 강남구 테헤란로 123',       # company_address
        1                                  # is_active
    )
    
    result = db_manager.execute_query(insert_query, params)
    if result:
        logger.info(f"회사 '{company_domain}' 데이터 삽입 성공")
        return True
    else:
        logger.error(f"회사 '{company_domain}' 데이터 삽입 실패")
        return False

def insert_server(db_manager, company_domain, device_id, server_name=None, ip_address=None):
    """서버 정보 삽입 또는 확인 - 안전한 동적 필드명 사용"""
    logger.info(f"서버 정보 설정: {company_domain}, {device_id}")
    
    # DB 스키마 정보 가져오기
    server_table = DB_SCHEMA.get("server_table", "servers")
    server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
    server_domain_field = DB_SCHEMA.get("server_domain_field", "company_domain")
    
    try:
        # 실제 테이블 구조 확인
        table_structure = db_manager.fetch_all(f"DESCRIBE {server_table}")
        column_names = [row[0] for row in table_structure] if table_structure else []
        
        # 디바이스 ID를 저장하는 컬럼명 결정 (우선순위 순서)
        device_column = None
        possible_columns = ['iphost', 'server_iphost', 'device_id', 'server_ip', 'ip_address']
        
        for col in possible_columns:
            if col in column_names:
                device_column = col
                break
        
        if not device_column:
            logger.error(f"servers 테이블에서 디바이스 ID 컬럼을 찾을 수 없습니다. 사용 가능한 컬럼: {column_names}")
            return None
        
        logger.info(f"사용할 디바이스 컬럼: {device_column}")
        
        # 서버가 이미 존재하는지 확인
        check_query = f"""
        SELECT {server_id_field} FROM {server_table} 
        WHERE {server_domain_field} = %s AND {device_column} = %s
        """
        exists = db_manager.fetch_one(check_query, (company_domain, device_id))
        
        if exists:
            logger.info(f"서버 '{device_id}'가 이미 존재합니다.")
            return exists[0]  # 서버 ID 반환
        
        # 새 서버 삽입
        insert_columns = [server_domain_field, device_column]
        insert_values = [company_domain, device_id]
        
        # 선택적 컬럼들 추가
        if 'server_name' in column_names and server_name:
            insert_columns.append('server_name')
            insert_values.append(server_name)
        
        insert_query = f"""
        INSERT INTO {server_table} 
        ({', '.join(insert_columns)}) 
        VALUES ({', '.join(['%s'] * len(insert_values))})
        """
        
        result = db_manager.execute_query(insert_query, tuple(insert_values))
        if result:
            logger.info(f"서버 '{device_id}' 데이터 삽입 성공")
            
            # 서버 ID 조회
            server_id_query = f"""
            SELECT {server_id_field} FROM {server_table} 
            WHERE {server_domain_field} = %s AND {device_column} = %s
            ORDER BY {server_id_field} DESC LIMIT 1
            """
            server_id_result = db_manager.fetch_one(server_id_query, (company_domain, device_id))
            
            if server_id_result:
                return server_id_result[0]
            else:
                logger.error("서버 ID를 조회할 수 없습니다.")
                return None
        else:
            logger.error(f"서버 '{device_id}' 데이터 삽입 실패")
            return None
            
    except Exception as e:
        logger.error(f"서버 정보 설정 중 오류: {e}")
        return None

def insert_configurations(db_manager, company_domain, server_id):
    """설정 정보 삽입 - 동적 필드명 사용"""
    logger.info(f"설정 정보 등록: {company_domain}, 서버ID={server_id}")
    
    # DB 스키마 정보 가져오기
    server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
    
    # 시스템 리소스 설정
    sys_resources = COLLECTION["system_resources"]
    sys_resources_value = ",".join(sys_resources)
    
    # 제외 위치 설정
    excluded_locations = COLLECTION["excluded_locations"]
    excluded_locations_value = ",".join(excluded_locations)
    
    # 제외 디바이스 설정
    excluded_devices = ["nhnacademy-Inspiron-14-5420", "24e124743d011875", "24e124136d151368"]
    excluded_devices_value = ",".join(excluded_devices)
    
    # 제외 애플리케이션 설정
    excluded_apps = []  # 필요한 애플리케이션 추가
    excluded_apps_value = ",".join(excluded_apps)
    
    # 설정 정보
    configs = [
        # 시스템 리소스 설정 (전체 회사 설정)
        {
            "company_domain": company_domain,
            "server_id": None,
            "config_type": "system_resources",
            "config_key": "list",
            "config_value": sys_resources_value,
            "is_active": True
        },
        # 제외 위치 설정 (전체 회사 설정)
        {
            "company_domain": company_domain,
            "server_id": None,
            "config_type": "excluded_locations",
            "config_key": "list",
            "config_value": excluded_locations_value,
            "is_active": True
        },
        # 제외 디바이스 설정 (전체 회사 설정)
        {
            "company_domain": company_domain,
            "server_id": None,
            "config_type": "excluded_devices",
            "config_key": "list",
            "config_value": excluded_devices_value,
            "is_active": True
        },
        # 제외 애플리케이션 설정 (전체 회사 설정)
        {
            "company_domain": company_domain,
            "server_id": None,
            "config_type": "excluded_apps",
            "config_key": "list",
            "config_value": excluded_apps_value,
            "is_active": True
        },
        # 수집 간격 설정 (서버별 설정)
        {
            "company_domain": company_domain,
            "server_id": server_id,
            "config_type": "collection",
            "config_key": "interval_minutes",
            "config_value": str(COLLECTION["regular_interval"]),
            "is_active": True
        }
    ]
    
    # 각 설정 삽입
    for config in configs:
        # 이미 존재하는지 확인
        check_query = f"""
        SELECT id FROM configurations 
        WHERE company_domain = %s AND 
              ({server_id_field} IS NULL OR {server_id_field} = %s) AND 
              config_type = %s AND 
              config_key = %s
        """
        check_params = (
            config["company_domain"],
            config["server_id"],
            config["config_type"],
            config["config_key"]
        )
        
        exists = db_manager.fetch_one(check_query, check_params)
        
        if exists:
            # 기존 설정 업데이트
            update_query = """
            UPDATE configurations 
            SET config_value = %s, is_active = %s, updated_at = NOW()
            WHERE id = %s
            """
            
            update_params = (
                config["config_value"],
                config["is_active"],
                exists[0]
            )
            
            result = db_manager.execute_query(update_query, update_params)
            if result:
                logger.info(f"설정 '{config['config_type']}.{config['config_key']}' 업데이트 성공")
            else:
                logger.error(f"설정 '{config['config_type']}.{config['config_key']}' 업데이트 실패")
        else:
            # 새 설정 삽입
            insert_query = f"""
            INSERT INTO configurations 
            (company_domain, {server_id_field}, config_type, config_key, config_value, is_active) 
            VALUES 
            (%s, %s, %s, %s, %s, %s)
            """
            
            insert_params = (
                config["company_domain"],
                config["server_id"],
                config["config_type"],
                config["config_key"],
                config["config_value"],
                config["is_active"]
            )
            
            result = db_manager.execute_query(insert_query, insert_params)
            if result:
                logger.info(f"설정 '{config['config_type']}.{config['config_key']}' 삽입 성공")
            else:
                logger.error(f"설정 '{config['config_type']}.{config['config_key']}' 삽입 실패")
    
    return True

def discover_influxdb_metadata():
    """InfluxDB에서 사용 가능한 companyDomain과 deviceId 검색"""
    try:
        from influxdb_client import InfluxDBClient
        from config.settings import INFLUXDB 
        
        client = InfluxDBClient(
            url=INFLUXDB["url"],
            token=INFLUXDB["token"],
            org=INFLUXDB["org"],
            timeout=60000
        )
        query_api = client.query_api()
        
        # companyDomain 검색
        domain_query = f'''
        from(bucket: "{INFLUXDB["bucket"]}")
        |> range(start: -90d)
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => exists r.companyDomain and r.companyDomain != "")
        |> group(columns: ["companyDomain"])
        |> distinct(column: "companyDomain")
        |> keep(columns: ["_value"])
        '''
        
        domains = []
        results = query_api.query(domain_query)
        for table in results:
            for record in table.records:
                domains.append(record.get_value())
        
        # deviceId 검색 (도메인별)
        devices_by_domain = {}
        for domain in domains:
            device_query = f'''
            from(bucket: "{INFLUXDB["bucket"]}")
            |> range(start: -90d)
            |> filter(fn: (r) => r["origin"] == "server_data")
            |> filter(fn: (r) => r["companyDomain"] == "{domain}")
            |> filter(fn: (r) => exists r.deviceId and r.deviceId != "")
            |> group(columns: ["deviceId"])
            |> distinct(column: "deviceId")
            |> keep(columns: ["_value"])
            '''
            
            devices = []
            device_results = query_api.query(device_query)
            for table in device_results:
                for record in table.records:
                    devices.append(record.get_value())
            
            devices_by_domain[domain] = devices
        
        client.close()
        return domains, devices_by_domain
        
    except Exception as e:
        logger.error(f"InfluxDB 메타데이터 검색 오류: {e}")
        return [], {}
def insert_aggregation_configurations(db_manager, company_domain):
    """집계 방식 기본 설정 등록"""
    logger.info(f"집계 방식 설정 등록: {company_domain}")
    
    # 기본 집계 설정
    aggregation_configs = [
        {
            "resource_type": "cpu",
            "method": "max",
            "reason": "CPU 피크 사용률이 시스템 부하 예측에 중요"
        },
        {
            "resource_type": "mem", 
            "method": "last",
            "reason": "메모리는 현재 점유 상태가 가장 의미있음"
        },
        {
            "resource_type": "disk",
            "method": "last", 
            "reason": "디스크 사용률은 누적적 특성으로 현재 상태가 중요"
        }
    ]
    
    for config in aggregation_configs:
        success = db_manager.save_aggregation_config(
            company_domain,
            config["resource_type"],
            config["method"], 
            config["reason"]
        )
        
        if success:
            logger.info(f"  {config['resource_type']}: {config['method']} 방식 등록 완료")
        else:
            logger.error(f"  {config['resource_type']} 설정 등록 실패")
def setup_all(company_domain=None, device_id=None):
    """전체 설정 실행"""
    logger.info("===== JVM 메트릭 시스템 초기 설정 시작 =====")
    
    # 자동 검색 활성화
    if not company_domain or not device_id:
        logger.info("InfluxDB에서 사용 가능한 도메인 및 디바이스 검색 중...")
        domains, devices_by_domain = discover_influxdb_metadata()
        
        if domains:
            logger.info(f"사용 가능한 도메인: {domains}")
            
            # 도메인 선택 (기본: 첫 번째 발견된 도메인)
            if not company_domain:
                company_domain = domains[0]
                logger.info(f"도메인 자동 선택: {company_domain}")
                
            # 디바이스 선택 (기본: 선택된 도메인의 첫 번째 디바이스)
            if not device_id and company_domain in devices_by_domain:
                available_devices = devices_by_domain[company_domain]
                if available_devices:
                    device_id = available_devices[0]
                    logger.info(f"디바이스 자동 선택: {device_id}")
        
        # 자동 감지 실패 시 기본값 사용
        if not company_domain:
            company_domain = COMPANY_DOMAIN
            logger.warning(f"도메인 감지 실패, 기본값 사용: {company_domain}")
        
        if not device_id:
            device_id = "192.168.71.74"
            logger.warning(f"디바이스 감지 실패, 기본값 사용: {device_id}")
    
    # 데이터베이스 연결
    db_manager = DatabaseManager()
    
    try:
        # 1. 데이터베이스 테이블 생성
        if not create_tables(db_manager):
            logger.error("테이블 생성 실패, 프로세스를 중단합니다.")
            return False
        
        # 2. 회사 정보 설정
        if not insert_company(db_manager, company_domain):
            logger.error("회사 정보 설정 실패, 프로세스를 중단합니다.")
            return False
        
        # 3. 서버 정보 설정 (디바이스 ID 사용)
        server_id = insert_server(db_manager, company_domain, device_id)
        if not server_id:
            logger.error("서버 정보 설정 실패, 프로세스를 중단합니다.")
            return False
        
        # 4. 설정 정보 등록
        if not insert_configurations(db_manager, company_domain, server_id):
            logger.error("설정 정보 등록 실패, 프로세스를 중단합니다.")
            return False
        
        # ⭐ 5. 집계 방식 설정 등록 (새로 추가)
        insert_aggregation_configurations(db_manager, company_domain)
        
        # 설정된 값을 환경 변수에 저장
        os.environ['JVM_METRICS_COMPANY'] = company_domain
        os.environ['JVM_METRICS_DEVICE_ID'] = device_id
        
        logger.info(f"===== JVM 메트릭 시스템 초기 설정 완료 =====")
        logger.info(f"회사 도메인: {company_domain}, 디바이스 ID: {device_id}")
        return True
    
    except Exception as e:
        logger.error(f"초기 설정 중 오류 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
    finally:
        db_manager.close()

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='JVM 메트릭 시스템 초기화')
    parser.add_argument('--company', help='회사 도메인 (지정하지 않으면 자동 감지)')
    parser.add_argument('--device', help='디바이스 ID (지정하지 않으면 자동 감지)')
    parser.add_argument('--auto', action='store_true', help='자동 감지 사용 (명시적으로 지정된 값 무시)')
    
    args = parser.parse_args()
    
    company_domain = None if args.auto else args.company
    device_id = None if args.auto else args.device
    
    if setup_all(company_domain, device_id):
        print(f"JVM 메트릭 시스템 초기 설정이 성공적으로 완료되었습니다.")
        print(f"회사 도메인: {os.environ.get('JVM_METRICS_COMPANY')}")
        print(f"디바이스 ID: {os.environ.get('JVM_METRICS_DEVICE_ID')}")
    else:
        print("JVM 메트릭 시스템 초기 설정 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main()