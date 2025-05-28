"""
JVM 메트릭 기반 시스템 리소스 예측 - 데이터베이스 연결 관리
"""
import mysql.connector
from mysql.connector import Error
from config.settings import MYSQL,DB_SCHEMA
from core.logger import logger

class DatabaseManager:
    """데이터베이스 연결 및 쿼리 관리 클래스"""
    
    def __init__(self):
        """초기화 함수"""
        self.connection = None
        self.server_table = DB_SCHEMA.get("server_table", "servers")
        self.server_id_field = DB_SCHEMA.get("server_id_field", "server_no")
        self.server_ip_field = DB_SCHEMA.get("server_ip_field", "server_iphost")
        self.server_domain_field = DB_SCHEMA.get("server_domain_field", "company_domain")
        self.connect()
    def get_active_companies(self):
        """활성화된 모든 회사 목록 조회"""
        query = "SELECT company_domain FROM companies WHERE is_active = 1"
        results = self.fetch_all(query)
        if results:
            return [row[0] for row in results]
        return []
    def get_aggregation_config(self, company_domain):
        """회사별 집계 설정 조회 (configurations 테이블에서)"""
        
        # DB에서 회사별 설정 조회
        query = """
        SELECT config_key, config_value FROM configurations 
        WHERE company_domain = %s AND config_type = 'aggregation' AND is_active = TRUE
        """
        
        results = self.fetch_all(query, (company_domain,))
        
        if results:
            # DB 설정이 있으면 사용
            db_config = {}
            for row in results:
                config_key, config_value = row
                # JSON 형태로 저장된 경우 파싱
                try:
                    import json
                    if config_value.startswith('{'):
                        db_config[config_key] = json.loads(config_value)
                    else:
                        db_config[config_key] = {"method": config_value}
                except:
                    db_config[config_key] = {"method": config_value}
            
            logger.info(f"회사 '{company_domain}' 맞춤 집계 설정 적용: {db_config}")
            return db_config
        else:
            # 없으면 기본 설정 사용
            from config.settings import RESOURCE_AGGREGATION
            logger.debug(f"회사 '{company_domain}' 기본 집계 설정 사용")
            return RESOURCE_AGGREGATION
    
    def save_aggregation_config(self, company_domain, resource_type, method, reason=None):
        """집계 설정 저장"""
        import json
        
        config_value = json.dumps({
            "method": method,
            "reason": reason or f"{resource_type} 리소스 최적 집계 방식"
        })
        
        query = """
        INSERT INTO configurations 
        (company_domain, server_no, config_type, config_key, config_value, is_active)
        VALUES (%s, NULL, 'aggregation', %s, %s, TRUE)
        ON DUPLICATE KEY UPDATE 
        config_value = VALUES(config_value), 
        updated_at = NOW()
        """
        
        return self.execute_query(query, (company_domain, resource_type, config_value))
    def get_or_create_server_for_device(self, company_domain, device_id):
        """디바이스 ID에 대한 서버 조회 또는 자동 생성 - 동적 필드명 사용"""
        # 1. 디바이스 ID와 매핑된 서버 찾기
        server_id = self.get_server_by_device_id(company_domain, device_id)
        if server_id:
            return server_id
        
        # 2. 없으면 새 서버 자동 생성
        query = f"""
        INSERT INTO {self.server_table} 
        ({self.server_domain_field}, {self.server_ip_field}) 
        VALUES (%s, %s)
        """
        
        params = (company_domain, device_id)
        
        if self.execute_query(query, params):
            # 생성된 서버 ID 조회
            id_query = f"""
            SELECT {self.server_id_field} FROM {self.server_table} 
            WHERE {self.server_domain_field} = %s AND {self.server_ip_field} = %s
            ORDER BY {self.server_id_field} DESC LIMIT 1
            """
            result = self.fetch_one(id_query, (company_domain, device_id))
            if result:
                logger.info(f"디바이스 '{device_id}'에 대한 서버 자동 생성 성공. 서버 ID: {result[0]}")
                return result[0]
        
        logger.error(f"디바이스 '{device_id}'에 대한 서버 생성 실패")
        return None
    def connect(self):
        """MySQL 데이터베이스에 연결"""
        try:
            self.connection = mysql.connector.connect(
                host=MYSQL["host"],
                port=MYSQL["port"],
                user=MYSQL["user"],
                password=MYSQL["password"],
                database=MYSQL["database"]
            )
            if self.connection.is_connected():
                logger.debug("MySQL 데이터베이스에 성공적으로 연결됨")
                return True
        except Error as e:
            logger.error(f"MySQL 연결 오류: {e}")
            return False
    
    def close(self):
        """MySQL 연결 종료"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.debug("MySQL 연결이 종료됨")
    
    def execute_query(self, query, params=None, many=False, retry_count=3):
        """쿼리 실행 (연결 자동 복구 기능 추가)"""
        for attempt in range(retry_count):
            if not self.connection or not self.connection.is_connected():
                if not self.connect():
                    return False
            
            cursor = None
            try:
                cursor = self.connection.cursor(buffered=True)
                
                if many and params:
                    # 배치 데이터 확인
                    if not params:
                        logger.warning("배치 데이터가 비어있습니다.")
                        return False
                    cursor.executemany(query, params)
                elif params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                self.connection.commit()
                logger.debug("쿼리 실행 성공")
                
                # 중요: 커서 닫기 전에 반환
                if cursor:
                    cursor.close()
                return True
                
            except Exception as e:
                logger.error(f"쿼리 실행 오류: {e}")
                
                try:
                    # 롤백 시도
                    if self.connection and self.connection.is_connected():
                        self.connection.rollback()
                except Exception as rollback_error:
                    logger.error(f"롤백 오류: {rollback_error}")
                
                # 연결 끊김 오류인 경우 재연결 시도
                if attempt < retry_count - 1:
                    logger.info(f"연결 재시도 중... (시도 {attempt + 1}/{retry_count})")
                    try:
                        if self.connection:
                            try:
                                self.connection.close()
                            except:
                                pass
                        self.connect()  # 새 연결 설정
                    except Exception as connect_error:
                        logger.error(f"재연결 오류: {connect_error}")
                
                # 마지막으로 커서 닫기 시도
                if cursor:
                    try:
                        cursor.close()
                    except:
                        pass
                        
                return False
            
    def get_configuration(self, company_domain, config_type, config_key=None, default_value=None):
        """통합 설정 값 조회 함수"""
        if config_key:
            query = """
            SELECT config_value FROM configurations 
            WHERE company_domain = %s AND config_type = %s AND config_key = %s 
            AND is_active = TRUE
            ORDER BY id DESC LIMIT 1
            """
            params = (company_domain, config_type, config_key)
        else:
            query = """
            SELECT config_key, config_value FROM configurations 
            WHERE company_domain = %s AND config_type = %s
            AND is_active = TRUE
            ORDER BY id DESC
            """
            params = (company_domain, config_type)
            
        results = self.fetch_all(query, params)
        
        if not results:
            return default_value
            
        if config_key:
            return results[0][0]  # 단일 값 반환
        else:
            # 키-값 딕셔너리 반환
            return {row[0]: row[1] for row in results}

    def fetch_all(self, query, params=None):
        """SELECT 쿼리 실행 및 모든 결과 반환"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return []
        
        cursor = self.connection.cursor(buffered=True)
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            return cursor.fetchall()
        except Error as e:
            logger.error(f"쿼리 실행 오류: {e}")
            return []
        finally:
            cursor.close()
    
    def fetch_one(self, query, params=None):
        """SELECT 쿼리 실행 및 첫 결과 반환"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return None
        
        cursor = self.connection.cursor(buffered=True)
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            return cursor.fetchone()
        except Error as e:
            logger.error(f"쿼리 실행 오류: {e}")
            return None
        finally:
            cursor.close()
    
    def check_company_exists(self, company_domain):
        """회사 도메인 존재 여부 확인"""
        query = "SELECT 1 FROM companies WHERE company_domain = %s"
        result = self.fetch_one(query, (company_domain,))
        return result is not None
    
    def get_server_id(self, company_domain, server_code):
        """호환성 유지를 위한 메서드"""
        # 기존 코드와의 호환성을 위해 server_code를 server_iphost로 취급
        return self.get_server_by_device_id(company_domain, server_code)
    
    def get_server_by_device_id(self, company_domain, device_id):
        """회사 도메인과 device_id로 서버 번호 조회 - 동적 필드명 사용"""
        query = f"""
        SELECT {self.server_id_field} FROM {self.server_table} 
        WHERE {self.server_domain_field} = %s AND {self.server_ip_field} = %s
        """
        result = self.fetch_one(query, (company_domain, device_id))
        return result[0] if result else None
    
    def get_system_resources(self, company_domain):
        """시스템 리소스 목록 조회"""
        config_value = self.get_configuration(company_domain, 'system_resources', 'list')
        return config_value.split(',') if config_value else ['cpu', 'mem', 'disk']
    
    def get_excluded_devices(self, company_domain):
        """제외할 디바이스 목록 조회"""
        config_value = self.get_configuration(company_domain, 'excluded_devices', 'list')
        return config_value.split(',') if config_value else []

    def get_excluded_locations(self, company_domain):
        """제외할 location 목록 조회"""
        config_value = self.get_configuration(company_domain, 'excluded_locations', 'list')
        return config_value.split(',') if config_value else ['diskio', 'net', 'sensors', 'swap', 'system', 'http', 'unknown_service', '입구', 'jvm']
    
    def get_excluded_apps(self, company_domain):
        """제외할 애플리케이션 목록 조회"""
        config_value = self.get_configuration(company_domain, 'excluded_apps', 'list')
        return config_value.split(',') if config_value else []
    def update_aggregation_config_from_env(self, company_domain):
        """환경변수에서 집계 설정 업데이트"""
        from config.settings import TIME_ALIGNMENT_CONFIG
        
        aggregation_updates = [
            ('cpu', TIME_ALIGNMENT_CONFIG.get('cpu_aggregation_method', 'max')),
            ('mem', TIME_ALIGNMENT_CONFIG.get('memory_aggregation_method', 'last')),
            ('disk', TIME_ALIGNMENT_CONFIG.get('disk_aggregation_method', 'last')),
        ]
        
        for resource_type, method in aggregation_updates:
            success = self.save_aggregation_config(
                company_domain, 
                resource_type, 
                method, 
                f"환경변수 기반 {resource_type} 집계 설정"
            )
            
            if success:
                logger.info(f"집계 설정 업데이트: {resource_type} -> {method}")
            else:
                logger.warning(f"집계 설정 업데이트 실패: {resource_type}")

    def get_time_alignment_settings(self, company_domain):
        """시간 정렬 설정 조회"""
        settings = {}
        
        # 시간 정렬 관련 설정들 조회
        alignment_configs = [
            'time_alignment_minutes',
            'aggregation_window_seconds', 
            'time_tolerance_minutes',
            'max_prediction_age_hours'
        ]
        
        for config_key in alignment_configs:
            value = self.get_configuration(company_domain, 'time_alignment', config_key)
            if value:
                settings[config_key] = int(value)
        
        return settings
