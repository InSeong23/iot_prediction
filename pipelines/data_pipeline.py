"""
데이터 수집 및 전처리 파이프라인 - ConfigManager 연동
- 기존 DataCollector, DataPreprocessor를 파이프라인으로 래핑
- 초기 수집, 정기 수집, 전처리를 하나의 플로우로 관리
"""
import os
import sys
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config_manager import ConfigManager
from core.logger import logger
from core.db import DatabaseManager

class BasePipeline(ABC):
    """파이프라인 기본 클래스"""
    
    def __init__(self, config_dict=None):
        # dict 설정을 ConfigWrapper로 감싸기
        if isinstance(config_dict, dict):
            from core.config_manager import ConfigWrapper
            self.config = ConfigWrapper(config_dict)
        else:
            self.config = config_dict or {}
        self._db_manager = None
    
    def get_db_manager(self):
        """데이터베이스 매니저 지연 로딩"""
        if self._db_manager is None:
            self._db_manager = DatabaseManager()
        return self._db_manager
    
    @abstractmethod
    def execute(self, **kwargs) -> bool:
        """파이프라인 실행"""
        pass
    
    def __del__(self):
        if self._db_manager:
            self._db_manager.close()

class DataPipeline(BasePipeline):
    """데이터 수집 및 전처리 파이프라인"""
    
    def execute(self, mode: str = 'regular', **kwargs) -> bool:
        """
        데이터 파이프라인 실행
        
        Args:
            mode: 'initial' | 'regular' | 'preprocess-only'
            **kwargs: 추가 파라미터 (period, overlap_minutes 등)
        """
        try:
            logger.info(f"데이터 파이프라인 실행 시작: 모드={mode}")
            
            if mode == 'initial':
                return self._run_initial_collection(**kwargs)
            elif mode == 'regular':
                return self._run_regular_collection(**kwargs)
            elif mode == 'preprocess-only':
                return self._run_preprocessing(**kwargs)
            else:
                logger.error(f"지원하지 않는 모드: {mode}")
                return False
                
        except Exception as e:
            logger.error(f"데이터 파이프라인 실행 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _run_initial_collection(self, **kwargs) -> bool:
        """초기 데이터 수집"""
        logger.info("초기 데이터 수집 시작")
        
        try:
            from data.collector import DataCollector
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain or not device_id:
                logger.error("회사 도메인 또는 디바이스 ID가 설정되지 않았습니다.")
                return False
            
            # 수집 기간 설정
            period = kwargs.get('period', self.config.get('initial_data_period', '7d'))
            start_time = kwargs.get('start_time')
            end_time = kwargs.get('end_time')
            
            # DataCollector 초기화 (ConfigManager 전달)
            collector = DataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            # 수집 실행
            if start_time and end_time:
                # 명시적 시간 범위
                success = collector.collect_data_for_range(start_time, end_time)
            else:
                # 기간 기반 수집
                success = collector.collect_initial_data(period)
            
            if success:
                logger.info("초기 데이터 수집 완료, 전처리 시작")
                # 수집 후 자동으로 전처리 실행
                return self._run_preprocessing()
            else:
                logger.error("초기 데이터 수집 실패")
                return False
            
        except Exception as e:
            logger.error(f"초기 데이터 수집 오류: {e}")
            return False
    
    def _run_regular_collection(self, **kwargs) -> bool:
        """정기 데이터 수집"""
        logger.info("정기 데이터 수집 시작")
        
        try:
            from data.collector import DataCollector
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # 중복 허용 시간 설정
            overlap_minutes = kwargs.get(
                'overlap_minutes', 
                self.config.get('data_collection_overlap', 10)
            )
            
            # DataCollector 초기화 (ConfigManager 전달)
            collector = DataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            # 정기 수집 실행
            success = collector.collect_recent_data(overlap_minutes)
            
            if success:
                logger.info("정기 데이터 수집 완료, 전처리 시작")
                # 수집 후 자동으로 전처리 실행
                return self._run_preprocessing()
            else:
                logger.warning("정기 데이터 수집 실패")
                return False
            
        except Exception as e:
            logger.error(f"정기 데이터 수집 오류: {e}")
            return False
    
    def _run_preprocessing(self, **kwargs) -> bool:
        """데이터 전처리"""
        logger.info("데이터 전처리 시작")
        
        try:
            from data.preprocessor import DataPreprocessor
            
            # ConfigManager에서 설정 가져오기
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            server_id = self.config.get_server_id()
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            # 전처리 시간 범위 (선택적)
            start_time = kwargs.get('start_time')
            end_time = kwargs.get('end_time')
            
            # DataPreprocessor 초기화 (ConfigManager 전달)
            preprocessor = DataPreprocessor(
                config_manager=self.config,
                company_domain=company_domain,
                server_id=server_id,
                device_id=device_id
            )
            
            # 전처리 파이프라인 실행
            success = preprocessor.run_preprocessing_pipeline(start_time, end_time)
            
            if success:
                logger.info("데이터 전처리 완료")
                return True
            else:
                logger.error("데이터 전처리 실패")
                return False
            
        except Exception as e:
            logger.error(f"데이터 전처리 오류: {e}")
            return False
    
    def check_recent_data(self, minutes: int = 30) -> bool:
        """최근 데이터 확인"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 최근 데이터 확인을 건너뜁니다.")
                return False
            
            # 지정된 시간 이내 데이터 확인
            recent_time = datetime.now() - timedelta(minutes=minutes)
            
            device_filter = ""
            params = [company_domain, server_id, recent_time]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 시스템 리소스 데이터 확인
            query = f"""
            SELECT COUNT(*) FROM system_resources 
            WHERE company_domain = %s AND {db.server_id_field} = %s 
            AND time > %s{device_filter}
            """
            
            result = db.fetch_one(query, tuple(params))
            
            if result and result[0] > 0:
                logger.info(f"최근 {minutes}분 내 데이터 {result[0]}개 확인")
                return True
            else:
                logger.warning(f"최근 {minutes}분 내 데이터가 없습니다.")
                return False
            
        except Exception as e:
            logger.warning(f"최근 데이터 확인 오류: {e}")
            return False
    
    def get_collection_status(self) -> dict:
        """데이터 수집 상태 정보 반환"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                return {
                    'status': 'error',
                    'message': '회사 도메인 또는 서버 ID가 설정되지 않음'
                }
            
            device_filter = ""
            params = [company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 시스템 리소스 데이터 현황
            sys_query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(time) as earliest_time,
                MAX(time) as latest_time
            FROM system_resources 
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            sys_result = db.fetch_one(sys_query, tuple(params))
            
            # JVM 메트릭 데이터 현황
            jvm_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT application) as app_count,
                MIN(time) as earliest_time,
                MAX(time) as latest_time
            FROM jvm_metrics 
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            jvm_result = db.fetch_one(jvm_query, tuple(params))
            
            # 영향도 데이터 현황
            impact_query = f"""
            SELECT 
                COUNT(*) as total_records,
                MIN(time) as earliest_time,
                MAX(time) as latest_time
            FROM application_impact 
            WHERE company_domain = %s AND {db.server_id_field} = %s{device_filter}
            """
            
            impact_result = db.fetch_one(impact_query, tuple(params))
            
            return {
                'status': 'success',
                'company_domain': company_domain,
                'device_id': device_id,
                'system_resources': {
                    'total_records': sys_result[0] if sys_result else 0,
                    'earliest_time': sys_result[1] if sys_result else None,
                    'latest_time': sys_result[2] if sys_result else None
                },
                'jvm_metrics': {
                    'total_records': jvm_result[0] if jvm_result else 0,
                    'app_count': jvm_result[1] if jvm_result else 0,
                    'earliest_time': jvm_result[2] if jvm_result else None,
                    'latest_time': jvm_result[3] if jvm_result else None
                },
                'application_impact': {
                    'total_records': impact_result[0] if impact_result else 0,
                    'earliest_time': impact_result[1] if impact_result else None,
                    'latest_time': impact_result[2] if impact_result else None
                }
            }
            
        except Exception as e:
            logger.error(f"수집 상태 조회 오류: {e}")
            return {
                'status': 'error',
                'message': str(e)
            }
    
    def auto_discover_and_setup(self) -> bool:
        """신규 엔티티 자동 감지 및 등록"""
        logger.info("신규 엔티티 자동 감지 시작")
        
        try:
            # 기존 스크립트의 discover_influxdb_metadata 활용
            from scripts.setup import discover_influxdb_metadata
            
            domains, devices_by_domain = discover_influxdb_metadata()
            
            if not domains:
                logger.warning("InfluxDB에서 감지된 도메인이 없습니다.")
                return False
            
            db = self.get_db_manager()
            setup_count = 0
            
            # 감지된 엔티티 등록
            for domain in domains:
                # 회사 도메인 등록 확인
                if not db.check_company_exists(domain):
                    logger.info(f"새 회사 도메인 감지: {domain}")
                    from scripts.setup import insert_company
                    if insert_company(db, domain):
                        setup_count += 1
                
                # 디바이스 등록 확인
                if domain in devices_by_domain:
                    for device_id in devices_by_domain[domain]:
                        server_id = db.get_server_by_device_id(domain, device_id)
                        if not server_id:
                            logger.info(f"새 디바이스 감지: {domain}, {device_id}")
                            from scripts.setup import insert_server, insert_configurations
                            
                            server_id = insert_server(db, domain, device_id)
                            if server_id:
                                insert_configurations(db, domain, server_id)
                                setup_count += 1
            
            logger.info(f"신규 엔티티 등록 완료: {setup_count}개")
            return setup_count > 0
            
        except Exception as e:
            logger.error(f"신규 엔티티 자동 감지 오류: {e}")
            return False
    
    def cleanup_old_data(self, days: int = 30) -> bool:
        """오래된 데이터 정리"""
        logger.info(f"{days}일 이전 데이터 정리 시작")
        
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            device_id = self.config.get('device_id')
            
            if not company_domain or not server_id:
                logger.warning("회사 도메인 또는 서버 ID가 없어 데이터 정리를 건너뜁니다.")
                return False
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            device_filter = ""
            params = [cutoff_date, company_domain, server_id]
            
            if device_id:
                device_filter = " AND device_id = %s"
                params.append(device_id)
            
            # 정리할 테이블들
            tables = [
                'system_resources',
                'jvm_metrics', 
                'application_impact',
                'feature_data'
            ]
            
            total_deleted = 0
            
            for table in tables:
                delete_query = f"""
                DELETE FROM {table} 
                WHERE time < %s 
                AND company_domain = %s 
                AND {db.server_id_field} = %s{device_filter}
                """
                
                # 삭제 전 개수 확인
                count_query = f"""
                SELECT COUNT(*) FROM {table} 
                WHERE time < %s 
                AND company_domain = %s 
                AND {db.server_id_field} = %s{device_filter}
                """
                
                count_result = db.fetch_one(count_query, tuple(params))
                before_count = count_result[0] if count_result else 0
                
                if before_count > 0:
                    if db.execute_query(delete_query, tuple(params)):
                        total_deleted += before_count
                        logger.info(f"{table} 테이블에서 {before_count}개 레코드 삭제")
                    else:
                        logger.error(f"{table} 테이블 데이터 삭제 실패")
            
            logger.info(f"데이터 정리 완료: 총 {total_deleted}개 레코드 삭제")
            return True
            
        except Exception as e:
            logger.error(f"데이터 정리 오류: {e}")
            return False