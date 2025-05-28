"""
JVM 메트릭 기반 시스템 리소스 예측 - 데이터 수집 모듈  
- InfluxDB에서 데이터 수집 및 MySQL에 저장
- 모든 데이터를 분 단위 정각으로 정렬
- 동일 분 내 여러 데이터 집계 처리
"""
import logging
import uuid
from datetime import datetime, timedelta
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxRecord

from core.config_manager import ConfigManager
from core.db import DatabaseManager
from core.logger import logger

class DataCollector:
    """InfluxDB에서 데이터를 수집하고 MySQL에 저장하는 클래스"""
    
    def __init__(self, config_manager=None, company_domain=None, server_code=None, device_id=None):
        """초기화 함수 - ConfigManager 우선 사용"""
        # ConfigManager 초기화
        if config_manager:
            self.config = config_manager
        else:
            self.config = ConfigManager()
            # 전달받은 파라미터로 설정 업데이트
            if company_domain:
                self.config.set('company_domain', company_domain)
            if device_id:
                self.config.set('device_id', device_id)
        
        # InfluxDB 설정 가져오기
        influxdb_config = self.config.get_influxdb_config()
        
        self.influx_client = InfluxDBClient(
            url=influxdb_config['url'],
            token=influxdb_config['token'],
            org=influxdb_config['org'],
            timeout=self.config.get('influxdb_timeout', 60) * 1000
        )
        self.query_api = self.influx_client.query_api()
        self.db_manager = DatabaseManager()
        self.batch_id = str(uuid.uuid4())
        
        # 설정값들 가져오기
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        self.influxdb_bucket = influxdb_config['bucket']
        self.resource_location = self.config.get('resource_location', 'server_resource_data')
        self.service_location = self.config.get('service_location', 'service_resource_data')
        self.gateway_field = self.config.get('gateway_field', 'gatewayId')
        self.time_interval_minutes = self.config.get('time_interval_minutes', 5)
        
        # 서버 ID 설정
        self.server_id = self.config.get_server_id()
        if not self.server_id:
            logger.warning(f"서버 ID를 찾을 수 없습니다. 디바이스: {self.device_id}")
        
        # 집계 설정 및 제외 목록 가져오기
        self.aggregation_config = self.db_manager.get_aggregation_config(self.company_domain)
        self.system_resources = self.config.get_system_resources()
        self.excluded_locations = self.config.get_excluded_devices()
        
        logger.info(f"데이터 수집기 초기화 완료, 배치 ID: {self.batch_id}")
        logger.info(f"회사: {self.company_domain}, 서버ID: {self.server_id}, 디바이스: {self.device_id}")
    
    def __del__(self):
        """소멸자: 연결 종료"""
        if hasattr(self, 'influx_client') and self.influx_client:
            self.influx_client.close()
        if hasattr(self, 'db_manager') and self.db_manager:
            self.db_manager.close()
    
    def align_time_to_minute(self, time_val):
        """타임스탬프를 분 단위 정각으로 정렬  """
        if isinstance(time_val, str):
            time_val = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
        
        # 타임존 정보를 제거하고 분 단위 정각으로 정렬
        if time_val.tzinfo is not None:
            # UTC 시간을 로컬 시간으로 변환 후 타임존 제거
            time_val = time_val.replace(tzinfo=None)
        
        # 초와 마이크로초를 0으로 설정하여 분 단위 정각으로 정렬
        return time_val.replace(second=0, microsecond=0)
    
    def aggregate_minute_data(self, batch_data, resource_type):
        """분 단위로 정렬된 데이터를 집계"""
        if not batch_data:
            return []
        
        # 시간별 그룹화
        time_groups = {}
        for data in batch_data:
            aligned_time = self.align_time_to_minute(data[2])  # time 컬럼
            key = (data[0], data[1], aligned_time, data[3], data[4])  # company, server, time, resource, measurement
            
            if key not in time_groups:
                time_groups[key] = []
            time_groups[key].append(data[5])  # value 컬럼
        
        # 집계된 데이터 생성
        aggregated_data = []
        for key, values in time_groups.items():
            company, server, aligned_time, resource, measurement = key
            aggregated_value = self.aggregate_values_by_resource(resource_type, values)
            
            # 원본 데이터 구조 유지
            aggregated_data.append((
                company, server, aligned_time, resource, measurement,
                aggregated_value, self.device_id or '', self.batch_id
            ))
        
        return aggregated_data
    
    def is_application(self, gateway_id):
        """gateway_id가 애플리케이션인지 확인"""
        # 시스템 리소스이면 False
        if gateway_id in self.system_resources:
            return False
        # 제외 게이트웨이이면 False
        excluded_gateways = self.config.get('excluded_gateways', [])
        if gateway_id in excluded_gateways:
            return False
        return True
    
    def get_last_collection_time(self):
        """마지막 데이터 수집 시간 조회"""
        if not self.server_id:
            return datetime.now() - timedelta(days=7)
            
        query = f"""
        SELECT MAX(time) as last_time FROM (
            SELECT MAX(time) as time FROM jvm_metrics 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
            UNION ALL
            SELECT MAX(time) as time FROM system_resources 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s
        ) as combined
        """
        
        result = self.db_manager.fetch_one(query, (
            self.company_domain, self.server_id, 
            self.company_domain, self.server_id
        ))
        
        if result and result[0]:
            return result[0]
        
        # 기본값으로 설정된 기간만큼 이전 반환
        default_period = self.config.get('initial_data_period', '7d')
        value = int(default_period[:-1])
        unit = default_period[-1].lower()
        
        if unit == 'd':
            return datetime.now() - timedelta(days=value)
        elif unit == 'h':
            return datetime.now() - timedelta(hours=value)
        else:
            return datetime.now() - timedelta(days=7)
    
    def collect_data_for_range(self, start_time, end_time=None):
        """지정된 시간 범위의 데이터 수집 및 저장"""
        if end_time is None:
            end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        if isinstance(start_time, datetime):
            start_time = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        if isinstance(end_time, datetime):
            end_time = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        logger.info(f"기간 {start_time}부터 {end_time}까지의 데이터 수집 시작")
        
        try:
            # 시스템 리소스 데이터 수집
            sys_success = self.collect_system_resources(start_time, end_time)
            
            # JVM 메트릭 데이터 수집
            jvm_success = self.collect_jvm_metrics(start_time, end_time)
            
            if sys_success and jvm_success:
                logger.info("데이터 수집 완료")
                self.check_collected_data()
                return True
            else:
                logger.warning(f"데이터 수집 부분 실패: 시스템={sys_success}, JVM={jvm_success}")
                return False
                
        except Exception as e:
            logger.error(f"데이터 수집 중 오류 발생: {e}")
            return False
    
    def collect_initial_data(self, period=None):
        """초기 데이터 수집"""
        if period is None:
            period = self.config.get('initial_data_period', '7d')
        
        # period 문자열을 파싱
        try:
            value = int(period[:-1])
            unit = period[-1].lower()
        except (ValueError, IndexError):
            logger.error(f"잘못된 기간 형식: {period}")
            return False
        
        now = datetime.utcnow()
        
        if unit == 'm':
            start_time = now - timedelta(minutes=value)
        elif unit == 'h':
            start_time = now - timedelta(hours=value)
        elif unit == 'd':
            start_time = now - timedelta(days=value)
        elif unit == 'w':
            start_time = now - timedelta(weeks=value)
        else:
            logger.error(f"지원하지 않는 기간 단위: {unit}")
            return False
        
        return self.collect_data_for_range(start_time, now)
    
    def collect_recent_data(self, overlap_minutes=None):
        """최근 데이터 수집 (마지막 수집 시간 이후)"""
        if overlap_minutes is None:
            overlap_minutes = self.config.get('data_collection_overlap', 10)
            
        # 마지막 수집 시간 조회
        last_time = self.get_last_collection_time()
        
        # 중복 허용을 위해 지정된 시간만큼 이전부터 수집
        start_time = last_time - timedelta(minutes=overlap_minutes)
        end_time = datetime.utcnow()
        
        logger.info(f"최근 데이터 수집: {start_time} ~ {end_time}")
        
        return self.collect_data_for_range(start_time, end_time)
    
    def align_time_to_interval(self, time_val, minutes=None):
        """타임스탬프를 지정된 분 간격으로 정렬 (1분인 경우 분 단위 정각)"""
        if minutes is None:
            minutes = self.time_interval_minutes
            
        if isinstance(time_val, str):
            time_val = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
        
        # 타임존 정보 제거
        if time_val.tzinfo is not None:
            time_val = time_val.replace(tzinfo=None)
        
        # 1분 간격인 경우 분 단위 정각으로 정렬  
        if minutes == 1:
            return time_val.replace(second=0, microsecond=0)
        
        # 다른 간격인 경우 기존 로직 사용
        aligned_minute = (time_val.minute // minutes) * minutes
        return time_val.replace(minute=aligned_minute, second=0, microsecond=0)
    
    def aggregate_values_by_resource(self, resource_type, values):
        """리소스 타입별 최적 집계 방식"""
        if not values:
            return 0
        
        config = self.aggregation_config.get(resource_type, {"method": "average"})
        method = config["method"]
        
        if method == "max":
            return max(values)
        elif method == "min":
            return min(values)
        elif method == "last":
            return values[-1]
        elif method == "first":
            return values[0]
        elif method == "median":
            import statistics
            return statistics.median(values)
        else:  # "average" or 기타
            return sum(values) / len(values)
    
    def collect_system_resources(self, start_time, end_time):
        """시스템 리소스 데이터 수집 및 저장  """
        logger.info("시스템 리소스 데이터 수집 시작")
        
        # 디바이스 ID 필터
        device_filter = ""
        if self.device_id:
            device_filter = f' and r["deviceId"] == "{self.device_id}"'
        
        # 리소스 측정 항목
        from config.settings import RESOURCE_MEASUREMENTS
        
        success_count = 0
        total_count = 0
        
        # 각 리소스 유형에 대해 처리
        for resource_type in self.system_resources:
            if resource_type not in RESOURCE_MEASUREMENTS:
                logger.warning(f"지원하지 않는 리소스 타입: {resource_type}")
                continue
            
            for measurement in RESOURCE_MEASUREMENTS[resource_type]:
                total_count += 1
                logger.info(f"수집 중: {resource_type}.{measurement}")
                
                # InfluxDB 쿼리 작성
                query = f'''
                from(bucket: "{self.influxdb_bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["origin"] == "server_data")
                |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}"{device_filter})
                |> filter(fn: (r) => r["location"] == "{self.resource_location}")
                |> filter(fn: (r) => r["{self.gateway_field}"] == "{resource_type}")
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => r["_field"] == "value")
                '''
                
                try:
                    # 쿼리 실행 및 데이터 처리
                    if self._process_system_resource_query(query, resource_type, measurement):
                        success_count += 1
                except Exception as e:
                    logger.error(f"시스템 리소스 {resource_type}.{measurement} 수집 오류: {e}")
        
        logger.info(f"시스템 리소스 수집 완료: {success_count}/{total_count} 성공")
        return success_count > 0
    
    def _process_system_resource_query(self, query, resource_type, measurement):
        """시스템 리소스 쿼리 처리  """
        results = self.query_api.query(query)
        batch_data = []
        
        # 결과 처리
        for table in results:
            for record in table.records:
                time_val = record.get_time()
                # 분 단위 정각으로 정렬
                aligned_time = self.align_time_to_minute(time_val)
                
                device_id = record.values.get('deviceId', '')
                company_domain = record.values.get('companyDomain', self.company_domain)
                
                # CPU usage_idle → usage_user 변환
                if resource_type == "cpu" and measurement == "usage_idle":
                    value = 100.0 - record.get_value()
                    final_measurement = "usage_user"
                else:
                    value = record.get_value()
                    final_measurement = measurement
                
                batch_data.append((
                    company_domain, self.server_id, aligned_time,
                    resource_type, final_measurement, value,
                    device_id, self.batch_id
                ))
        
        if not batch_data:
            logger.warning(f"데이터 없음: {resource_type}.{measurement}")
            return False
        
        # 분 단위 집계 처리
        aggregated_data = self.aggregate_minute_data(batch_data, resource_type)
        
        # 데이터베이스에 저장
        return self._save_system_resource_batch(aggregated_data, resource_type, measurement)
    
    def _save_system_resource_batch(self, batch_data, resource_type, measurement):
        """시스템 리소스 배치 데이터 저장"""
        if not batch_data:
            return False
            
        insert_query = f"""
        INSERT INTO system_resources 
        (company_domain, {self.db_manager.server_id_field}, time, resource_type, measurement, value, device_id, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 배치 크기 제한
        batch_size = self.config.get('batch_size', 1000)
        success_count = 0
        
        for i in range(0, len(batch_data), batch_size):
            chunk = batch_data[i:i + batch_size]
            if self.db_manager.execute_query(insert_query, chunk, many=True):
                success_count += len(chunk)
        
        if success_count > 0:
            logger.info(f"{resource_type}.{measurement}: {success_count}개 저장 완료")
            return True
        else:
            logger.error(f"{resource_type}.{measurement}: 저장 실패")
            return False
    
    def collect_jvm_metrics(self, start_time, end_time):
        """JVM 메트릭 데이터 수집 및 저장  """
        logger.info("JVM 메트릭 데이터 수집 시작")
        
        # 디바이스 ID 필터
        device_filter = ""
        if self.device_id:
            device_filter = f' and r["deviceId"] == "{self.device_id}"'
        
        # 애플리케이션 목록 조회
        applications = self._discover_applications(start_time, end_time, device_filter)
        if not applications:
            logger.warning("수집할 애플리케이션이 없습니다.")
            return False
        
        # JVM 메트릭 목록
        from config.settings import JVM_METRICS
        
        success_count = 0
        total_count = len(applications) * len(JVM_METRICS)
        
        # 각 애플리케이션별로 메트릭 수집
        for app in applications:
            for metric in JVM_METRICS:
                try:
                    if self._collect_jvm_metric(start_time, end_time, device_filter, app, metric):
                        success_count += 1
                except Exception as e:
                    logger.error(f"JVM 메트릭 {app}.{metric} 수집 오류: {e}")
        
        logger.info(f"JVM 메트릭 수집 완료: {success_count}/{total_count} 성공")
        return success_count > 0
    
    def _discover_applications(self, start_time, end_time, device_filter):
        """애플리케이션 목록 자동 발견"""
        query = f'''
        from(bucket: "{self.influxdb_bucket}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}"{device_filter})
        |> filter(fn: (r) => r["location"] == "{self.service_location}")
        |> group(columns: ["{self.gateway_field}"])
        |> distinct(column: "{self.gateway_field}")
        '''
        
        try:
            results = self.query_api.query(query)
            applications = []
            
            for table in results:
                for record in table.records:
                    gateway_id = record.values.get('_value')
                    if gateway_id and self.is_application(gateway_id):
                        applications.append(gateway_id)
            
            logger.info(f"발견된 애플리케이션: {applications}")
            return applications
            
        except Exception as e:
            logger.error(f"애플리케이션 목록 조회 오류: {e}")
            return []
    
    def _collect_jvm_metric(self, start_time, end_time, device_filter, app, metric):
        """단일 JVM 메트릭 수집  """
        query = f'''
        from(bucket: "{self.influxdb_bucket}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}"{device_filter})
        |> filter(fn: (r) => r["location"] == "{self.service_location}")
        |> filter(fn: (r) => r["{self.gateway_field}"] == "{app}")
        |> filter(fn: (r) => r["_measurement"] == "{metric}")
        |> filter(fn: (r) => r["_field"] == "value")
        '''
        
        results = self.query_api.query(query)
        batch_data = []
        
        # 결과 처리
        for table in results:
            for record in table.records:
                time_val = record.get_time()
                # 분 단위 정각으로 정렬
                aligned_time = self.align_time_to_minute(time_val)
                value = record.get_value()
                
                device_id = record.values.get('deviceId', '')
                company_domain = record.values.get('companyDomain', self.company_domain)
                
                batch_data.append((
                    company_domain, self.server_id, aligned_time,
                    app, metric, value, device_id, self.batch_id
                ))
        
        if not batch_data:
            return False
        
        # 시간별 그룹화 및 평균 계산 (같은 분 내 여러 데이터)
        aligned_data = {}
        for data in batch_data:
            key = (data[0], data[1], data[2], data[3], data[4])  # company, server, time, app, metric
            if key not in aligned_data:
                aligned_data[key] = [data[5]]
            else:
                aligned_data[key].append(data[5])
        
        # 평균값으로 최종 배치 데이터 생성
        final_batch = []
        for key, values in aligned_data.items():
            avg_value = sum(values) / len(values)
            company, server, time_val, application, metric_type = key
            
            final_batch.append((
                company, server, time_val, application,
                metric_type, avg_value, self.device_id or '', self.batch_id
            ))
        
        # 데이터베이스에 저장
        return self._save_jvm_metric_batch(final_batch, app, metric)
    
    def _save_jvm_metric_batch(self, batch_data, app, metric):
        """JVM 메트릭 배치 데이터 저장"""
        if not batch_data:
            return False
            
        insert_query = f"""
        INSERT INTO jvm_metrics 
        (company_domain, {self.db_manager.server_id_field}, time, application, metric_type, value, device_id, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 배치 크기 제한
        batch_size = self.config.get('batch_size', 1000)
        success_count = 0
        
        for i in range(0, len(batch_data), batch_size):
            chunk = batch_data[i:i + batch_size]
            if self.db_manager.execute_query(insert_query, chunk, many=True):
                success_count += len(chunk)
        
        if success_count > 0:
            logger.debug(f"{app}.{metric}: {success_count}개 저장 완료")
            return True
        else:
            logger.error(f"{app}.{metric}: 저장 실패")
            return False
    
    def check_collected_data(self):
        """수집된 데이터 샘플 확인"""
        logger.info("수집된 데이터 확인")
        
        if not self.server_id:
            logger.warning("서버 ID가 없어 데이터 확인 건너뜀")
            return False
        
        try:
            # 시스템 리소스 데이터 확인
            sys_count_query = f"""
            SELECT COUNT(*) FROM system_resources 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND batch_id = %s
            """
            
            # JVM 메트릭 데이터 확인
            jvm_count_query = f"""
            SELECT COUNT(*) FROM jvm_metrics 
            WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND batch_id = %s
            """
            
            sys_count = self.db_manager.fetch_one(sys_count_query, (self.company_domain, self.server_id, self.batch_id))
            jvm_count = self.db_manager.fetch_one(jvm_count_query, (self.company_domain, self.server_id, self.batch_id))
            
            sys_total = sys_count[0] if sys_count else 0
            jvm_total = jvm_count[0] if jvm_count else 0
            
            logger.info(f"수집 결과: 시스템 리소스 {sys_total}개, JVM 메트릭 {jvm_total}개")
            
            # 애플리케이션 목록 조회
            if jvm_total > 0:
                app_query = f"""
                SELECT DISTINCT application FROM jvm_metrics 
                WHERE company_domain = %s AND {self.db_manager.server_id_field} = %s AND batch_id = %s
                """
                
                apps = self.db_manager.fetch_all(app_query, (self.company_domain, self.server_id, self.batch_id))
                if apps:
                    app_list = [app[0] for app in apps]
                    logger.info(f"수집된 애플리케이션: {', '.join(app_list)}")
            
            return sys_total > 0 and jvm_total > 0
            
        except Exception as e:
            logger.error(f"데이터 확인 중 오류 발생: {e}")
            return False