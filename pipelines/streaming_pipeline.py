"""
스트리밍 데이터 파이프라인 - InfluxDB 직접 처리 및 파일 캐싱
기존 MySQL 저장 방식 대신 실시간 처리 및 캐싱 사용
"""
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.data_pipeline import BasePipeline
from core.logger import logger

class StreamingDataPipeline(BasePipeline):
    """스트리밍 데이터 파이프라인 - 파일 캐싱 기반"""
    
    def execute(self, mode: str = 'training', **kwargs) -> bool:
        """
        스트리밍 파이프라인 실행
        
        Args:
            mode: 'training' | 'inference' | 'cache-cleanup' | 'status'
            **kwargs: 추가 파라미터
        """
        try:
            logger.info(f"스트리밍 파이프라인 실행: 모드={mode}")
            
            if mode == 'training':
                return self._run_training_pipeline(**kwargs)
            elif mode == 'inference':
                return self._run_inference_pipeline(**kwargs)
            elif mode == 'cache-cleanup':
                return self._run_cache_cleanup(**kwargs)
            elif mode == 'status':
                return self._check_pipeline_status(**kwargs)
            else:
                logger.error(f"지원하지 않는 모드: {mode}")
                return False
                
        except Exception as e:
            logger.error(f"스트리밍 파이프라인 실행 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _run_training_pipeline(self, **kwargs) -> bool:
        """학습용 데이터 파이프라인 실행"""
        logger.info("학습용 스트리밍 파이프라인 시작")
        
        try:
            # 스트리밍 수집기 초기화
            from data.streaming_collector import StreamingDataCollector
            
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            if not company_domain:
                logger.error("회사 도메인이 설정되지 않았습니다.")
                return False
            
            collector = StreamingDataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            # 훈련용 데이터 조회 (기본 3일)
            training_days = kwargs.get('days', 3)
            end_time = datetime.now()
            start_time = end_time - timedelta(days=training_days)
            
            logger.info(f"훈련 데이터 수집: {start_time} ~ {end_time}")
            jvm_df, sys_df = collector.get_training_data(start_time, end_time)
            
            if jvm_df.empty or sys_df.empty:
                logger.error("학습용 데이터가 충분하지 않습니다.")
                return False
            
            # 스트리밍 전처리기 초기화
            from data.streaming_preprocessor import StreamingPreprocessor
            
            preprocessor = StreamingPreprocessor(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            # 캐시 키 생성
            cache_key = f"{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
            
            # 영향도 계산 및 캐싱
            force_recalc = kwargs.get('force_recalculate', False)
            impact_df = preprocessor.calculate_and_cache_impacts(
                jvm_df, sys_df, cache_key, force_recalc
            )
            
            # 특성 생성 및 캐싱
            features_df = preprocessor.generate_and_cache_features(
                jvm_df, cache_key, force_recalc
            )
            
            # 결과 검증
            success = True
            if impact_df is None or impact_df.empty:
                logger.warning("영향도 데이터 생성 실패")
                success = False
            
            if features_df is None or features_df.empty:
                logger.warning("특성 데이터 생성 실패")
                success = False
            
            # 작업 로그 기록
            self._log_job_result('training_pipeline', success, {
                'cache_key': cache_key,
                'jvm_records': len(jvm_df),
                'sys_records': len(sys_df),
                'impact_records': len(impact_df) if impact_df is not None else 0,
                'feature_records': len(features_df) if features_df is not None else 0
            })
            
            if success:
                logger.info("학습용 스트리밍 파이프라인 완료")
            else:
                logger.warning("학습용 스트리밍 파이프라인 부분 실패")
            
            return success
            
        except Exception as e:
            logger.error(f"학습용 파이프라인 오류: {e}")
            return False
    
    def _run_inference_pipeline(self, **kwargs) -> bool:
        """추론용 데이터 파이프라인 실행"""
        logger.info("추론용 스트리밍 파이프라인 시작")
        
        try:
            # 스트리밍 수집기 초기화
            from data.streaming_collector import StreamingDataCollector
            
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            collector = StreamingDataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            # 최근 데이터 조회 (예측용)
            minutes = kwargs.get('minutes', 30)
            logger.info(f"최근 {minutes}분 데이터 수집")
            
            jvm_df, sys_df = collector.get_latest_data(minutes)
            
            if jvm_df.empty:
                logger.warning("최근 JVM 데이터가 없습니다.")
                return False
            
            # 간단한 전처리 (실시간)
            processed_data = self._process_inference_data(jvm_df, sys_df)
            
            # 작업 로그 기록
            self._log_job_result('inference_pipeline', True, {
                'jvm_records': len(jvm_df),
                'sys_records': len(sys_df),
                'processed_apps': len(processed_data) if processed_data else 0
            })
            
            logger.info("추론용 스트리밍 파이프라인 완료")
            return True
            
        except Exception as e:
            logger.error(f"추론용 파이프라인 오류: {e}")
            return False
    
    def _process_inference_data(self, jvm_df, sys_df) -> Optional[Dict]:
        """추론용 데이터 간단 전처리"""
        try:
            if jvm_df.empty:
                return None
            
            # 애플리케이션별 최근 메트릭 정리
            processed = {}
            
            # 애플리케이션별로 그룹화
            for app, app_data in jvm_df.groupby('application'):
                app_metrics = {}
                
                # 메트릭별 최근 값
                for _, row in app_data.iterrows():
                    app_metrics[row['metric_type']] = row['value']
                
                # 시간 특성 추가
                latest_time = app_data['time'].max()
                app_metrics['hour'] = latest_time.hour
                app_metrics['day_of_week'] = latest_time.weekday()
                app_metrics['is_weekend'] = 1 if latest_time.weekday() >= 5 else 0
                
                processed[app] = app_metrics
            
            return processed
            
        except Exception as e:
            logger.error(f"추론 데이터 전처리 오류: {e}")
            return None
    
    def _run_cache_cleanup(self, **kwargs) -> bool:
        """캐시 정리 실행"""
        logger.info("캐시 정리 파이프라인 시작")
        
        try:
            # 스트리밍 수집기와 전처리기의 캐시 정리
            from data.streaming_collector import StreamingDataCollector
            from data.streaming_preprocessor import StreamingPreprocessor
            
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            # 최대 보관 시간 (기본 48시간)
            max_age_hours = kwargs.get('max_age_hours', 48)
            
            # 수집기 캐시 정리
            collector = StreamingDataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            collector.cleanup_cache(max_age_hours)
            
            # 전처리기 캐시 정리
            preprocessor = StreamingPreprocessor(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            preprocessor.cleanup_cache(max_age_hours)
            
            # 캐시 메타데이터 정리
            self._cleanup_cache_metadata(max_age_hours)
            
            # 작업 로그 기록
            self._log_job_result('cache_cleanup', True, {
                'max_age_hours': max_age_hours,
                'cleanup_time': datetime.now().isoformat()
            })
            
            logger.info("캐시 정리 파이프라인 완료")
            return True
            
        except Exception as e:
            logger.error(f"캐시 정리 파이프라인 오류: {e}")
            return False
    
    def _cleanup_cache_metadata(self, max_age_hours: int):
        """데이터베이스 캐시 메타데이터 정리"""
        try:
            db = self.get_db_manager()
            
            # 만료된 캐시 메타데이터 삭제
            cleanup_query = """
            DELETE FROM cache_metadata 
            WHERE expires_at < NOW() OR created_at < DATE_SUB(NOW(), INTERVAL %s HOUR)
            """
            
            result = db.execute_query(cleanup_query, (max_age_hours,))
            
            if result:
                logger.info("캐시 메타데이터 정리 완료")
            else:
                logger.warning("캐시 메타데이터 정리 실패")
                
        except Exception as e:
            logger.error(f"캐시 메타데이터 정리 오류: {e}")
    
    def _check_pipeline_status(self, **kwargs) -> bool:
        """파이프라인 상태 확인"""
        logger.info("스트리밍 파이프라인 상태 확인")
        
        try:
            # 스트리밍 수집기와 전처리기 상태 확인
            from data.streaming_collector import StreamingDataCollector
            from data.streaming_preprocessor import StreamingPreprocessor
            
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            # 수집기 상태
            collector = StreamingDataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            collector_status = collector.get_cache_status()
            
            # 전처리기 상태
            preprocessor = StreamingPreprocessor(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            preprocessor_status = preprocessor.get_cache_status()
            
            # 최근 작업 로그 확인
            recent_jobs = self._get_recent_job_logs(hours=24)
            
            # 상태 정보 출력
            logger.info("=== 스트리밍 파이프라인 상태 ===")
            logger.info(f"회사: {company_domain}, 디바이스: {device_id}")
            logger.info(f"수집기 캐시: {collector_status.get('file_count', 0)}개 파일, "
                    f"{collector_status.get('total_size_mb', 0)}MB")
            logger.info(f"전처리기 캐시: 영향도 {preprocessor_status.get('impacts', {}).get('file_count', 0)}개, "
                    f"특성 {preprocessor_status.get('features', {}).get('file_count', 0)}개")
            logger.info(f"최근 24시간 작업: {len(recent_jobs)}개")
            
            # 최근 작업 요약
            if recent_jobs:
                successful_jobs = sum(1 for job in recent_jobs if job['job_status'] == 'completed')
                logger.info(f"  성공: {successful_jobs}/{len(recent_jobs)}")
                
                for job in recent_jobs[-5:]:  # 최근 5개 작업
                    # 이모티콘 대신 텍스트 사용
                    status = "[SUCCESS]" if job['job_status'] == 'completed' else "[FAILED]"
                    logger.info(f"  {status} {job['job_type']}: {job['start_time']}")
            
            return True
            
        except Exception as e:
            logger.error(f"파이프라인 상태 확인 오류: {e}")
            return False
    
    def _log_job_result(self, job_type: str, success: bool, details: Dict[str, Any]):
        """작업 결과 로그 기록"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            
            if not company_domain or not server_id:
                return
            
            # 작업 로그 삽입
            insert_query = """
            INSERT INTO job_logs 
            (company_domain, server_no, job_type, job_status, start_time, end_time, 
             records_processed, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            import json
            status = 'completed' if success else 'failed'
            now = datetime.now()
            
            # 처리된 레코드 수 계산
            records_processed = 0
            for key in ['jvm_records', 'sys_records', 'impact_records', 'feature_records']:
                if key in details:
                    records_processed += details[key]
            
            params = (
                company_domain,
                server_id,
                job_type,
                status,
                now,  # start_time (간소화)
                now,  # end_time
                records_processed,
                json.dumps(details)
            )
            
            db.execute_query(insert_query, params)
            
        except Exception as e:
            logger.error(f"작업 로그 기록 오류: {e}")
    
    def _get_recent_job_logs(self, hours: int = 24) -> list:
        """최근 작업 로그 조회"""
        try:
            db = self.get_db_manager()
            company_domain = self.config.get('company_domain')
            server_id = self.config.get_server_id()
            
            if not company_domain or not server_id:
                return []
            
            query = """
            SELECT job_type, job_status, start_time, end_time, records_processed, details
            FROM job_logs
            WHERE company_domain = %s AND server_no = %s
            AND start_time >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            ORDER BY start_time DESC
            """
            
            results = db.fetch_all(query, (company_domain, server_id, hours))
            
            if results:
                return [
                    {
                        'job_type': row[0],
                        'job_status': row[1],
                        'start_time': row[2],
                        'end_time': row[3],
                        'records_processed': row[4],
                        'details': row[5]
                    }
                    for row in results
                ]
            else:
                return []
                
        except Exception as e:
            logger.error(f"작업 로그 조회 오류: {e}")
            return []
    
    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """파이프라인 성능 메트릭 조회"""
        try:
            # 캐시 효율성 계산
            from data.streaming_collector import StreamingDataCollector
            from data.streaming_preprocessor import StreamingPreprocessor
            
            company_domain = self.config.get('company_domain')
            device_id = self.config.get('device_id')
            
            collector = StreamingDataCollector(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            preprocessor = StreamingPreprocessor(
                config_manager=self.config,
                company_domain=company_domain,
                device_id=device_id
            )
            
            collector_status = collector.get_cache_status()
            preprocessor_status = preprocessor.get_cache_status()
            
            # 최근 작업 성과
            recent_jobs = self._get_recent_job_logs(hours=24)
            
            success_rate = 0
            if recent_jobs:
                successful = sum(1 for job in recent_jobs if job['job_status'] == 'completed')
                success_rate = (successful / len(recent_jobs)) * 100
            
            avg_processing_time = 0
            if recent_jobs:
                processing_times = []
                for job in recent_jobs:
                    if job['start_time'] and job['end_time']:
                        duration = (job['end_time'] - job['start_time']).total_seconds()
                        processing_times.append(duration)
                
                if processing_times:
                    avg_processing_time = sum(processing_times) / len(processing_times)
            
            return {
                'pipeline_type': 'streaming',
                'company_domain': company_domain,
                'device_id': device_id,
                'cache_metrics': {
                    'collector': collector_status,
                    'preprocessor': preprocessor_status,
                    'total_files': (collector_status.get('file_count', 0) + 
                                  preprocessor_status.get('impacts', {}).get('file_count', 0) +
                                  preprocessor_status.get('features', {}).get('file_count', 0)),
                    'total_size_mb': (collector_status.get('total_size_mb', 0) + 
                                    preprocessor_status.get('total_size_mb', 0))
                },
                'performance_metrics': {
                    'recent_jobs_24h': len(recent_jobs),
                    'success_rate_percent': round(success_rate, 2),
                    'avg_processing_time_seconds': round(avg_processing_time, 2)
                },
                'status': 'healthy' if success_rate > 80 else 'warning' if success_rate > 50 else 'critical'
            }
            
        except Exception as e:
            logger.error(f"파이프라인 메트릭 조회 오류: {e}")
            return {
                'pipeline_type': 'streaming',
                'status': 'error',
                'error': str(e)
            }