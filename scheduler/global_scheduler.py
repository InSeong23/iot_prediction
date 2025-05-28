"""
JVM 메트릭 기반 시스템 리소스 예측 - 글로벌 멀티 테넌트 스케줄러
모든 회사를 자동 감지하여 독립적으로 처리하는 스케줄러
"""
import os
import sys
import time
import schedule
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.logger import logger, set_context
from core.db import DatabaseManager
from pipelines.data_pipeline import DataPipeline
from pipelines.model_pipeline import ModelPipeline
from pipelines.prediction_pipeline import PredictionPipeline
from scheduler.health_monitor import HealthMonitor

class CompanyProcessor:
    """개별 회사 처리기"""
    
    def __init__(self, company_domain: str, global_config: dict):
        """회사별 처리기 초기화"""
        self.company_domain = company_domain
        self.global_config = global_config
        self.devices = []
        self.db_manager = DatabaseManager()
        
        # 로그 컨텍스트 설정
        set_context(company_domain=company_domain)
        
        # 회사별 디바이스 목록 조회
        self._discover_devices()
        
        logger.info(f"회사 '{company_domain}' 처리기 초기화 완료, 디바이스 {len(self.devices)}개")
    
    def _discover_devices(self):
        """회사의 모든 디바이스 감지"""
        try:
            from influxdb_client import InfluxDBClient
            
            client = InfluxDBClient(
                url=self.global_config['influxdb_url'],
                token=self.global_config['influxdb_token'],
                org=self.global_config['influxdb_org'],
                timeout=60000
            )
            
            query_api = client.query_api()
            
            # 회사별 디바이스 조회
            device_query = f'''
            from(bucket: "{self.global_config['influxdb_bucket']}")
            |> range(start: -30d)
            |> filter(fn: (r) => r["origin"] == "server_data")
            |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}")
            |> filter(fn: (r) => exists r.deviceId and r.deviceId != "")
            |> group(columns: ["deviceId"])
            |> distinct(column: "deviceId")
            |> keep(columns: ["_value"])
            '''
            
            results = query_api.query(device_query)
            
            devices = []
            for table in results:
                for record in table.records:
                    device_id = record.get_value()
                    if device_id and device_id not in devices:
                        devices.append(device_id)
            
            # 제외 디바이스 필터링
            excluded_devices = self.db_manager.get_excluded_devices(self.company_domain)
            self.devices = [d for d in devices if d not in excluded_devices]
            
            client.close()
            
            logger.info(f"회사 '{self.company_domain}' 디바이스 {len(self.devices)}개 감지: {self.devices}")
            
        except Exception as e:
            logger.error(f"회사 '{self.company_domain}' 디바이스 감지 오류: {e}")
            self.devices = []
    
    def process_company_data(self) -> bool:
        """회사 전체 데이터 처리"""
        logger.info(f"회사 '{self.company_domain}' 데이터 처리 시작")
        
        if not self.devices:
            logger.warning(f"회사 '{self.company_domain}'에 처리할 디바이스가 없습니다")
            return False
        
        success_count = 0
        
        # 각 디바이스별 처리
        for device_id in self.devices:
            try:
                if self._process_device(device_id):
                    success_count += 1
                else:
                    logger.warning(f"디바이스 '{device_id}' 처리 실패")
            except Exception as e:
                logger.error(f"디바이스 '{device_id}' 처리 중 오류: {e}")
        
        # 회사 전체 모델 학습 (디바이스별 데이터 통합)
        if success_count > 0:
            self._train_company_models()
            self._predict_company_resources()
        
        logger.info(f"회사 '{self.company_domain}' 처리 완료: {success_count}/{len(self.devices)} 성공")
        return success_count > 0
    
    def _process_device(self, device_id: str) -> bool:
        """개별 디바이스 데이터 처리"""
        logger.info(f"디바이스 '{device_id}' 데이터 처리 시작")
        
        try:
            # 서버 ID 확인 또는 생성
            server_id = self.db_manager.get_server_by_device_id(self.company_domain, device_id)
            if not server_id:
                server_id = self._register_device(device_id)
                if not server_id:
                    return False
            
            # 로그 컨텍스트 업데이트
            set_context(company_domain=self.company_domain, device_id=device_id)
            
            # 데이터 수집 및 전처리
            data_pipeline = DataPipeline(self._create_device_config(device_id, server_id))
            if not data_pipeline.execute(mode='regular'):
                logger.error(f"디바이스 '{device_id}' 데이터 수집 실패")
                return False
            
            logger.info(f"디바이스 '{device_id}' 데이터 처리 완료")
            return True
            
        except Exception as e:
            logger.error(f"디바이스 '{device_id}' 처리 오류: {e}")
            return False
    
    def _register_device(self, device_id: str) -> Optional[int]:
        """새 디바이스 자동 등록"""
        try:
            from scripts.setup import insert_server
            server_id = insert_server(self.db_manager, self.company_domain, device_id)
            
            if server_id:
                logger.info(f"디바이스 '{device_id}' 자동 등록 완료, 서버 ID: {server_id}")
                return server_id
            else:
                logger.error(f"디바이스 '{device_id}' 자동 등록 실패")
                return None
                
        except Exception as e:
            logger.error(f"디바이스 '{device_id}' 등록 오류: {e}")
            return None
    
    def _create_device_config(self, device_id: str, server_id: int) -> dict:
        """디바이스별 설정 생성"""
        return {
            'company_domain': self.company_domain,
            'device_id': device_id,
            'server_id': server_id,
            **self.global_config
        }
    
    def _train_company_models(self):
        """회사 전체 모델 학습"""
        logger.info(f"회사 '{self.company_domain}' 모델 학습 시작")
        
        try:
            # 회사의 첫 번째 디바이스로 모델 학습 (통합 데이터 사용)
            if self.devices:
                device_id = self.devices[0]
                server_id = self.db_manager.get_server_by_device_id(self.company_domain, device_id)
                
                if server_id:
                    config = self._create_device_config(device_id, server_id)
                    
                    # 모델 학습 파이프라인
                    model_pipeline = ModelPipeline(config)
                    model_pipeline.execute(mode='all')
                    
                    logger.info(f"회사 '{self.company_domain}' 모델 학습 완료")
                    
        except Exception as e:
            logger.error(f"회사 '{self.company_domain}' 모델 학습 오류: {e}")
    
    def _predict_company_resources(self):
        """회사 전체 리소스 예측"""
        logger.info(f"회사 '{self.company_domain}' 리소스 예측 시작")
        
        try:
            # 각 디바이스별 예측 실행
            for device_id in self.devices:
                server_id = self.db_manager.get_server_by_device_id(self.company_domain, device_id)
                if server_id:
                    config = self._create_device_config(device_id, server_id)
                    
                    prediction_pipeline = PredictionPipeline(config)
                    prediction_pipeline.execute(mode='predict')
            
            logger.info(f"회사 '{self.company_domain}' 리소스 예측 완료")
            
        except Exception as e:
            logger.error(f"회사 '{self.company_domain}' 리소스 예측 오류: {e}")


class GlobalScheduler:
    """글로벌 멀티 테넌트 스케줄러"""
    

    def __init__(self):
        """글로벌 스케줄러 초기화"""
        self.is_running = False
        self.is_stopping = False
        self._start_time = datetime.now()  # 시작 시간 추가
        
        # 글로벌 설정 로드
        self.global_config = self._load_global_config()
        
        # DB 매니저 초기화
        self.db_manager = DatabaseManager()
        
        # 회사별 처리기 관리
        self.company_processors: Dict[str, CompanyProcessor] = {}
        
        # 헬스 모니터 초기화
        self.health_monitor = HealthMonitor(None)  # 글로벌 모니터링
        
        # 스케줄 설정
        self.schedule_config = {
            'discovery_interval': int(os.getenv('DISCOVERY_INTERVAL', '60')),      # 1시간마다 새 회사 감지
            'data_collection_interval': int(os.getenv('DATA_COLLECTION_INTERVAL', '30')),  # 30분마다 데이터 수집
            'model_training_interval': int(os.getenv('MODEL_TRAINING_INTERVAL', '360')),   # 6시간마다 모델 학습
            'prediction_interval': int(os.getenv('PREDICTION_INTERVAL', '60')),           # 1시간마다 예측
            'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', '15')),       # 15분마다 상태 확인
        }
        
        # 신호 처리기 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("글로벌 스케줄러 초기화 완료")
        logger.info(f"스케줄 설정: {self.schedule_config}")
    
    def _load_global_config(self) -> dict:
        """글로벌 설정 로드"""
        return {
            # InfluxDB 설정
            'influxdb_url': os.getenv('INFLUXDB_URL', 'http://localhost:8888'),
            'influxdb_token': os.getenv('INFLUXDB_TOKEN', 'g-W7W0j9AE4coriQfnhHGMDnDhTZGok8bgY1NnZ6Z0EnTOsFY3SWAqDTC5fYlQ9mYnbK_doR074-a4Dgck2AOQ=='),
            'influxdb_org': os.getenv('INFLUXDB_ORG', 'javame'),
            'influxdb_bucket': os.getenv('INFLUXDB_BUCKET', 'data'),
            
            # MySQL 설정
            'mysql_host': os.getenv('MYSQL_HOST', 's4.java21.net'),
            'mysql_port': int(os.getenv('MYSQL_PORT', '13306')),
            'mysql_user': os.getenv('MYSQL_USER', 'aiot02_team3'),
            'mysql_password': os.getenv('MYSQL_PASSWORD', 'ryL7LcSp@Yiz[bR7'),
            'mysql_database': os.getenv('MYSQL_DATABASE', 'aiot02_team3'),
            
            # 처리 설정
            'max_workers': int(os.getenv('MAX_WORKERS', '3')),  # 최대 동시 처리 회사 수
            'batch_size': int(os.getenv('BATCH_SIZE', '1000')),
        }
    
    def _signal_handler(self, signum, frame):
        """시스템 신호 처리"""
        logger.info(f"종료 신호 수신: {signum}")
        self.stop()
    
    def discover_and_register_companies(self):
        """InfluxDB에서 회사 자동 감지 및 등록"""
        logger.info("회사 자동 감지 및 등록 시작")
        
        try:
            from scripts.setup import discover_influxdb_metadata, insert_company
            
            # InfluxDB에서 메타데이터 감지
            domains, devices_by_domain = discover_influxdb_metadata()
            
            if not domains:
                logger.warning("감지된 회사 도메인이 없습니다")
                return
            
            # 새 회사 등록
            new_companies = 0
            for domain in domains:
                if not self.db_manager.check_company_exists(domain):
                    if insert_company(self.db_manager, domain):
                        logger.info(f"새 회사 등록: {domain}")
                        new_companies += 1
                
                # 회사별 처리기 생성 또는 업데이트
                if domain not in self.company_processors:
                    self.company_processors[domain] = CompanyProcessor(domain, self.global_config)
                    logger.info(f"회사 '{domain}' 처리기 생성")
                else:
                    # 기존 처리기의 디바이스 목록 갱신
                    self.company_processors[domain]._discover_devices()
            
            logger.info(f"회사 감지 완료: 총 {len(domains)}개, 신규 {new_companies}개")
            
        except Exception as e:
            logger.error(f"회사 감지 및 등록 오류: {e}")
    
    def run_all_companies_data_collection(self):
        """모든 회사 데이터 수집"""
        if self.is_stopping:
            return
        
        logger.info("전체 회사 데이터 수집 시작")
        
        if not self.company_processors:
            logger.warning("처리할 회사가 없습니다")
            return
        
        # 병렬 처리
        max_workers = min(self.global_config['max_workers'], len(self.company_processors))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 각 회사별로 데이터 수집 작업 제출
            future_to_company = {
                executor.submit(self._safe_process_company, company, processor): company 
                for company, processor in self.company_processors.items()
            }
            
            # 결과 수집
            success_count = 0
            for future in as_completed(future_to_company):
                company = future_to_company[future]
                try:
                    if future.result():
                        success_count += 1
                        logger.info(f"회사 '{company}' 처리 성공")
                    else:
                        logger.warning(f"회사 '{company}' 처리 실패")
                except Exception as e:
                    logger.error(f"회사 '{company}' 처리 중 예외: {e}")
        
        logger.info(f"전체 회사 데이터 수집 완료: {success_count}/{len(self.company_processors)} 성공")
    
    def _safe_process_company(self, company: str, processor: CompanyProcessor) -> bool:
        """안전한 회사 처리 (오류 격리)"""
        try:
            return processor.process_company_data()
        except Exception as e:
            logger.error(f"회사 '{company}' 처리 중 격리된 오류: {e}")
            return False
    
    def run_health_check(self):
        """전체 시스템 헬스 체크"""
        if self.is_stopping:
            return
        
        logger.info("전체 시스템 헬스 체크")
        
        try:
            # 글로벌 헬스 체크
            health_status = self.health_monitor.perform_health_check()
            
            if health_status['overall_status'] == 'healthy':
                logger.info("전체 시스템 상태 정상")
            elif health_status['overall_status'] == 'warning':
                logger.warning("전체 시스템 상태 주의")
            else:
                logger.error("전체 시스템 상태 이상")
            
            # 회사별 상태 확인
            for company, processor in self.company_processors.items():
                try:
                    if processor.devices:
                        logger.debug(f"회사 '{company}': 디바이스 {len(processor.devices)}개 활성")
                    else:
                        logger.warning(f"회사 '{company}': 활성 디바이스 없음")
                except Exception as e:
                    logger.error(f"회사 '{company}' 상태 확인 오류: {e}")
                    
        except Exception as e:
            logger.error(f"헬스 체크 오류: {e}")
    
    def setup_schedule(self):
        """작업 스케줄 설정"""
        logger.info("글로벌 스케줄 설정 중...")
        
        # 스케줄 초기화
        schedule.clear()
        
        # 회사 감지 및 등록 (1시간마다)
        schedule.every(self.schedule_config['discovery_interval']).minutes.do(
            self._safe_job_wrapper, self.discover_and_register_companies, "회사 감지"
        )
        
        # 전체 회사 데이터 수집 (30분마다)
        schedule.every(self.schedule_config['data_collection_interval']).minutes.do(
            self._safe_job_wrapper, self.run_all_companies_data_collection, "데이터 수집"
        )
        
        # 헬스 체크 (15분마다)
        schedule.every(self.schedule_config['health_check_interval']).minutes.do(
            self._safe_job_wrapper, self.run_health_check, "헬스 체크"
        )
        
        logger.info("글로벌 스케줄 설정 완료:")
        for job_name, interval in self.schedule_config.items():
            logger.info(f"  {job_name}: {interval}분마다")
    
    def _safe_job_wrapper(self, job_func, job_name: str):
        """작업 실행 래퍼 (안전한 실행)"""
        if self.is_stopping:
            return
        
        try:
            logger.debug(f"{job_name} 시작")
            job_func()
            logger.debug(f"{job_name} 완료")
        except Exception as e:
            logger.error(f"{job_name} 실행 중 예외 발생: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def run_initial_setup(self) -> bool:
        """초기 설정 실행"""
        logger.info("글로벌 초기 설정 시작")
        
        try:
            # 기본 DB 스키마 확인
            if not self.db_manager.connection or not self.db_manager.connection.is_connected():
                logger.error("데이터베이스 연결 실패")
                return False
            
            # 회사 자동 감지 및 등록
            self.discover_and_register_companies()
            
            # 즉시 한 번 데이터 수집 실행 (설정에 따라)
            run_immediate = os.getenv('RUN_IMMEDIATE', 'true').lower() == 'true'
            
            if run_immediate and self.company_processors:
                logger.info("초기 데이터 수집 실행")
                self.run_all_companies_data_collection()
            
            return True
            
        except Exception as e:
            logger.error(f"초기 설정 중 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def start(self):
        """글로벌 스케줄러 시작"""
        logger.info("글로벌 JVM 메트릭 예측 시스템 시작")
        
        try:
            # 초기 설정
            if not self.run_initial_setup():
                logger.error("초기 설정 실패로 인한 종료")
                return False
            
            # 스케줄 설정
            self.setup_schedule()
            
            # 헬스 모니터 시작
            self.health_monitor.start()
            
            # 스케줄 실행 시작
            self.is_running = True
            logger.info("글로벌 스케줄 실행 시작")
            
            # 메인 스케줄 루프
            self._run_schedule_loop()
            
            return True
            
        except KeyboardInterrupt:
            logger.info("사용자에 의한 종료")
            return True
        except Exception as e:
            logger.error(f"글로벌 스케줄러 실행 중 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            self.stop()
    
    def _run_schedule_loop(self):
        """스케줄 실행 메인 루프"""
        check_interval = 60  # 1분마다 스케줄 확인
        
        while self.is_running and not self.is_stopping:
            try:
                # 대기 중인 작업 실행
                schedule.run_pending()
                
                # 다음 실행까지 대기
                time.sleep(check_interval)
                
            except Exception as e:
                logger.error(f"스케줄 루프 중 오류: {e}")
                time.sleep(check_interval)
    
    def stop(self):
        """글로벌 스케줄러 중지"""
        logger.info("글로벌 스케줄러 중지 시작")
        
        self.is_stopping = True
        self.is_running = False
        
        try:
            # 헬스 모니터 중지
            if hasattr(self, 'health_monitor'):
                self.health_monitor.stop()
            
            # 스케줄 정리
            schedule.clear()
            
            # DB 연결 정리
            if hasattr(self, 'db_manager'):
                self.db_manager.close()
            
            logger.info("글로벌 스케줄러 정상 종료")
            
        except Exception as e:
            logger.error(f"글로벌 스케줄러 종료 중 오류: {e}")
    
    def get_status(self) -> dict:
        """전체 시스템 상태 정보"""
        try:
            return {
                'status': 'running' if self.is_running else 'stopped',
                'total_companies': len(self.company_processors),
                'companies': {
                    company: {
                        'devices': len(processor.devices),
                        'device_list': processor.devices
                    }
                    for company, processor in self.company_processors.items()
                },
                'schedule_config': self.schedule_config,
                'uptime': str(datetime.now() - self._start_time) if hasattr(self, '_start_time') else None
            }
            
        except Exception as e:
            logger.error(f"상태 조회 오류: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
