"""
개선된 데이터 수집기 - InfluxDB에서 직접 데이터 처리
MySQL 저장 없이 실시간 전처리 및 파일 캐싱
"""
import os
import json
import pickle
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from typing import Dict, List, Optional, Tuple

from core.config_manager import ConfigManager
from core.logger import logger

class StreamingDataCollector:
    """실시간 데이터 처리 수집기 - MySQL 저장 없음"""
    
    def __init__(self, config_manager=None, company_domain=None, device_id=None):
        """초기화"""
        if config_manager:
            self.config = config_manager
        else:
            self.config = ConfigManager()
            if company_domain:
                self.config.set('company_domain', company_domain)
            if device_id:
                self.config.set('device_id', device_id)
        
        # InfluxDB 설정
        influxdb_config = self.config.get_influxdb_config()
        self.influx_client = InfluxDBClient(
            url=influxdb_config['url'],
            token=influxdb_config['token'],
            org=influxdb_config['org'],
            timeout=60000
        )
        self.query_api = self.influx_client.query_api()
        
        # 기본 설정
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        self.influxdb_bucket = influxdb_config['bucket']
        
        # 캐시 디렉토리 설정
        self.cache_dir = os.path.join("cache", self.company_domain, self.device_id or "global")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f"스트리밍 데이터 수집기 초기화: {self.company_domain}/{self.device_id}")
    
    def __del__(self):
        """소멸자"""
        if hasattr(self, 'influx_client') and self.influx_client:
            self.influx_client.close()
    
    def get_training_data(self, start_time=None, end_time=None, cache_hours=6) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """학습용 데이터 조회 - 캐시 우선 사용"""
        if end_time is None:
            end_time = datetime.now()
        
        if start_time is None:
            start_time = end_time - timedelta(days=3)
        
        # 고정된 캐시 키 사용
        jvm_cache_file = os.path.join(self.cache_dir, "jvm_training_latest.pkl")
        sys_cache_file = os.path.join(self.cache_dir, "sys_training_latest.pkl")
        
        # 캐시된 데이터가 유효한지 확인 (cache_hours 시간 내)
        if self._is_cache_valid(jvm_cache_file, cache_hours) and self._is_cache_valid(sys_cache_file, cache_hours):
            logger.info("캐시된 학습 데이터 사용")
            try:
                with open(jvm_cache_file, 'rb') as f:
                    jvm_df = pickle.load(f)
                with open(sys_cache_file, 'rb') as f:
                    sys_df = pickle.load(f)
                return jvm_df, sys_df
            except Exception as e:
                logger.warning(f"캐시 로드 실패, 새로 조회: {e}")
        
        # InfluxDB에서 직접 조회
        logger.info(f"InfluxDB에서 데이터 조회: {start_time} ~ {end_time}")
        jvm_df = self._query_jvm_metrics(start_time, end_time)
        sys_df = self._query_system_resources(start_time, end_time)
        
        # 캐시에 저장 (덮어쓰기)
        if not jvm_df.empty and not sys_df.empty:
            try:
                with open(jvm_cache_file, 'wb') as f:
                    pickle.dump(jvm_df, f)
                with open(sys_cache_file, 'wb') as f:
                    pickle.dump(sys_df, f)
                logger.info("학습 데이터 캐시 업데이트 완료")
            except Exception as e:
                logger.warning(f"캐시 저장 실패: {e}")
        
        return jvm_df, sys_df
    
    def _is_cache_valid(self, cache_file: str, valid_hours: int) -> bool:
        """캐시 파일이 유효한지 확인"""
        if not os.path.exists(cache_file):
            return False
        
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return (datetime.now() - file_time).total_seconds() < valid_hours * 3600
    
    def _query_influxdb_data(self, start_time: datetime, end_time: datetime, query_type: str) -> pd.DataFrame:
        """InfluxDB에서 데이터 조회 (통합 함수)"""
        device_filter = f' and r["deviceId"] == "{self.device_id}"' if self.device_id else ""
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        base_query = f'''
        from(bucket: "{self.influxdb_bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}"{device_filter})
        '''
        
        if query_type == 'jvm':
            query = base_query + '''
            |> filter(fn: (r) => r["location"] == "service_resource_data")
            |> filter(fn: (r) => r["_field"] == "value")
            '''
        elif query_type == 'cpu':
            query = base_query + '''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "cpu")
            |> filter(fn: (r) => r["_measurement"] == "usage_idle")
            |> filter(fn: (r) => r["_field"] == "value")
            |> map(fn: (r) => ({ r with _value: 100.0 - r._value }))
            '''
        elif query_type == 'mem':
            query = base_query + '''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "mem")
            |> filter(fn: (r) => r["_measurement"] == "used_percent")
            |> filter(fn: (r) => r["_field"] == "value")
            '''
        elif query_type == 'disk':
            query = base_query + '''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "disk")
            |> filter(fn: (r) => r["_measurement"] == "used_percent")
            |> filter(fn: (r) => r["_field"] == "value")
            '''
        
        try:
            logger.debug(f"{query_type} 데이터 쿼리 실행: {start_str} ~ {end_str}")
            results = self.query_api.query(query)
            data = []
            
            for table in results:
                for record in table.records:
                    if query_type == 'jvm':
                        data.append({
                            'time': record.get_time(),
                            'application': record.values.get('gatewayId', ''),
                            'metric_type': record.get_measurement(),
                            'value': record.get_value(),
                            'device_id': record.values.get('deviceId', '')
                        })
                    else:
                        measurement = 'usage_user' if query_type == 'cpu' else 'used_percent'
                        data.append({
                            'time': record.get_time(),
                            'resource_type': query_type,
                            'measurement': measurement,
                            'value': record.get_value(),
                            'device_id': record.values.get('deviceId', '')
                        })
            
            df = pd.DataFrame(data)
            if not df.empty:
                df['time'] = pd.to_datetime(df['time'])
                logger.info(f"{query_type} 데이터 조회 완료: {len(df)}개")
            else:
                logger.warning(f"{query_type} 데이터 조회 결과 없음")
            
            return df
            
        except Exception as e:
            logger.error(f"{query_type} 데이터 조회 오류: {e}")
            return pd.DataFrame()
    
    def _query_jvm_metrics(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """InfluxDB에서 JVM 메트릭 조회"""
        return self._query_influxdb_data(start_time, end_time, 'jvm')
    
    def _query_system_resources(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """InfluxDB에서 시스템 리소스 조회"""
        all_data = []
        
        for resource_type in ['cpu', 'mem', 'disk']:
            df = self._query_influxdb_data(start_time, end_time, resource_type)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_latest_data(self, minutes=30) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """최근 데이터 조회 - 예측용"""
        end_time = datetime.utcnow()  # UTC 시간 사용
        start_time = end_time - timedelta(minutes=minutes)
        
        logger.info(f"최근 데이터 조회: {start_time} ~ {end_time} ({minutes}분)")
        
        # 최근 데이터는 캐시하지 않고 직접 조회
        jvm_df = self._query_jvm_metrics(start_time, end_time)
        sys_df = self._query_system_resources(start_time, end_time)
        
        # 데이터가 부족한 경우 기간 확장
        if jvm_df.empty or len(jvm_df) < 10:
            logger.warning(f"최근 {minutes}분 JVM 데이터 부족 ({len(jvm_df)}개), 기간 확장")
            
            # 최대 6시간까지 확장
            for extended_minutes in [60, 120, 360]:
                extended_start = end_time - timedelta(minutes=extended_minutes)
                jvm_df = self._query_jvm_metrics(extended_start, end_time)
                
                if not jvm_df.empty and len(jvm_df) >= 10:
                    logger.info(f"확장된 기간({extended_minutes}분)에서 JVM 데이터 발견: {len(jvm_df)}개")
                    # 시스템 데이터도 같은 기간으로 조회
                    sys_df = self._query_system_resources(extended_start, end_time)
                    break
            
            if jvm_df.empty:
                logger.error("확장된 기간에서도 JVM 데이터를 찾을 수 없습니다")
        
        logger.info(f"최근 데이터 조회 결과: JVM {len(jvm_df)}개, 시스템 {len(sys_df)}개")
        
        return jvm_df, sys_df
    
    def cleanup_cache(self, max_age_hours=48):
        """오래된 캐시 정리"""
        logger.info(f"캐시 정리 시작: {max_age_hours}시간 이상 파일 삭제")
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        deleted_count = 0
        
        try:
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                
                if os.path.isfile(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if file_time < cutoff_time:
                        os.remove(filepath)
                        deleted_count += 1
                        logger.debug(f"캐시 파일 삭제: {filename}")
            
            logger.info(f"캐시 정리 완료: {deleted_count}개 파일 삭제")
            
        except Exception as e:
            logger.error(f"캐시 정리 오류: {e}")
    
    def get_cache_status(self) -> Dict:
        """캐시 상태 조회"""
        try:
            cache_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')]
            total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) for f in cache_files)
            
            return {
                'cache_dir': self.cache_dir,
                'file_count': len(cache_files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'files': cache_files
            }
            
        except Exception as e:
            logger.error(f"캐시 상태 조회 오류: {e}")
            return {'error': str(e)}