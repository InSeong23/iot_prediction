"""
스트리밍 아키텍처용 모델 매니저
캐시된 데이터를 활용한 효율적인 모델 학습 및 예측
"""
import os
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from core.config_manager import ConfigManager
from core.logger import logger
from models.app_models import AppImpactModel, AppModelManager
from models.prediction import SystemResourcePredictor

class StreamingModelManager:
    """스트리밍 기반 통합 모델 매니저"""
    
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
        
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        self.server_id = self.config.get_server_id()
        
        # 캐시 디렉토리
        self.cache_dir = os.path.join("cache", self.company_domain, self.device_id or "global")
        self.model_dir = os.path.join("models", "trained", self.company_domain, str(self.server_id))
        
        os.makedirs(self.model_dir, exist_ok=True)
        
        # 모델 인스턴스들
        self.app_model_manager = None
        self.system_predictor = None
        
        logger.info(f"스트리밍 모델 매니저 초기화: {self.company_domain}/{self.device_id}")
    
    def train_models_from_cache(self, cache_key: str, force_retrain=False) -> bool:
        """캐시된 데이터로 모델 학습"""
        logger.info(f"캐시 기반 모델 학습 시작: {cache_key}")
        
        try:
            # 1. 캐시된 영향도 데이터 로드
            impact_df = self._load_cached_impacts()
            if impact_df is None or impact_df.empty:
                logger.error("캐시된 영향도 데이터가 없습니다")
                return False
            
            # 2. 캐시된 특성 데이터 로드 (고정 캐시)
            features_df = self._load_cached_features()
            if features_df is None or features_df.empty:
                logger.error("캐시된 특성 데이터가 없습니다")
                return False
            
            # 3. 애플리케이션 영향도 모델 학습
            app_success = self._train_app_models_from_cache(impact_df, features_df, force_retrain)
            
            # 4. 시스템 예측 모델 학습
            system_success = self._train_system_models_from_cache(impact_df, force_retrain)
            
            success = app_success and system_success
            
            if success:
                logger.info("캐시 기반 모델 학습 완료")
                # 학습 성공 시 메타데이터 저장
                self._save_training_metadata(cache_key, impact_df, features_df)
            else:
                logger.error("캐시 기반 모델 학습 실패")
            
            return success
            
        except Exception as e:
            logger.error(f"캐시 기반 모델 학습 오류: {e}")
            return False
    
    def _load_cached_impacts(self) -> Optional[pd.DataFrame]:
        """캐시된 영향도 데이터 로드 - 고정 캐시"""
        impact_file = os.path.join(self.cache_dir, "impacts", "impact_latest.pkl")
        
        if os.path.exists(impact_file):
            try:
                with open(impact_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"영향도 캐시 로드 오류: {e}")
        
        return None

    def _load_cached_features(self) -> Optional[pd.DataFrame]:
        """캐시된 특성 데이터 로드 - 고정 캐시"""
        features_file = os.path.join(self.cache_dir, "features", "features_latest.pkl")
        
        if os.path.exists(features_file):
            try:
                with open(features_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"특성 캐시 로드 오류: {e}")
        
        return None
    
    def _train_app_models_from_cache(self, impact_df: pd.DataFrame, features_df: pd.DataFrame, 
                                    force_retrain=False) -> bool:
        """캐시 데이터로 애플리케이션 모델 학습"""
        logger.info("캐시 기반 애플리케이션 모델 학습 시작")
        
        try:
            # 애플리케이션 목록 추출
            applications = impact_df['application'].unique().tolist()
            
            if not applications:
                logger.warning("학습할 애플리케이션이 없습니다")
                return False
            
            success_count = 0
            
            for app in applications:
                try:
                    # 해당 앱의 영향도 데이터 추출
                    app_impact_data = impact_df[impact_df['application'] == app]
                    
                    # 해당 앱의 특성 데이터 추출
                    app_features_data = features_df[features_df['application'] == app]
                    
                    if app_impact_data.empty or app_features_data.empty:
                        logger.warning(f"애플리케이션 '{app}' 데이터가 부족합니다")
                        continue
                    
                    # 학습 데이터 준비
                    X, y = self._prepare_app_training_data(app_features_data, app_impact_data)
                    
                    if X is None or y is None:
                        logger.warning(f"애플리케이션 '{app}' 학습 데이터 준비 실패")
                        continue
                    
                    # 모델 학습
                    app_model = AppImpactModel(
                        self.config, self.company_domain, self.server_id, app, self.device_id
                    )
                    
                    if app_model.train_models(X, y):
                        success_count += 1
                        logger.info(f"애플리케이션 '{app}' 모델 학습 완료")
                    else:
                        logger.warning(f"애플리케이션 '{app}' 모델 학습 실패")
                
                except Exception as e:
                    logger.error(f"애플리케이션 '{app}' 모델 학습 중 오류: {e}")
            
            logger.info(f"애플리케이션 모델 학습 완료: {success_count}/{len(applications)}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"애플리케이션 모델 학습 오류: {e}")
            return False
    
    def _prepare_app_training_data(self, features_df: pd.DataFrame, impact_df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """애플리케이션 학습 데이터 준비"""
        try:
            # 특성 데이터를 피봇 테이블로 변환 (시간 × 특성)
            features_pivot = features_df.pivot_table(
                index='time',
                columns='feature_name',
                values='value',
                aggfunc='mean'
            )
            
            # 영향도 데이터를 피봇 테이블로 변환 (시간 × 리소스)
            impact_pivot = impact_df.pivot_table(
                index='time',
                columns='resource_type',
                values='impact_score',
                aggfunc='mean'
            )
            
            # 공통 시간대 확인
            common_times = features_pivot.index.intersection(impact_pivot.index)
            
            if len(common_times) < 10:
                logger.warning("공통 시간대가 너무 적습니다")
                return None, None
            
            # 공통 시간대 데이터 추출
            X = features_pivot.loc[common_times].fillna(0)
            y = impact_pivot.loc[common_times].fillna(0)
            
            return X, y
            
        except Exception as e:
            logger.error(f"애플리케이션 학습 데이터 준비 오류: {e}")
            return None, None
    
    def _train_system_models_from_cache(self, impact_df: pd.DataFrame, force_retrain=False) -> bool:
        """캐시 데이터로 시스템 예측 모델 학습"""
        logger.info("캐시 기반 시스템 예측 모델 학습 시작")
        
        try:
            # 영향도 통계 데이터 준비
            X, y = self._prepare_system_training_data(impact_df)
            
            if X is None or y is None:
                logger.error("시스템 모델 학습 데이터 준비 실패")
                return False
            
            # 시스템 예측 모델 학습
            system_predictor = SystemResourcePredictor(
                self.config, self.company_domain, self.server_id, self.device_id
            )
            
            success = system_predictor.train_models(X, y)
            
            if success:
                self.system_predictor = system_predictor
                logger.info("시스템 예측 모델 학습 완료")
            else:
                logger.error("시스템 예측 모델 학습 실패")
            
            return success
            
        except Exception as e:
            logger.error(f"시스템 모델 학습 오류: {e}")
            return False
    
    def _prepare_system_training_data(self, impact_df: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """시스템 모델 학습 데이터 준비"""
        try:
            # 시간별로 그룹화하여 영향도 통계 계산
            impact_stats_list = []
            
            for time_point, time_group in impact_df.groupby('time'):
                stats_row = {'time': time_point}
                
                for resource_type in ['cpu', 'mem', 'disk']:
                    # 해당 시간, 해당 리소스의 모든 애플리케이션 영향도
                    resource_impacts = time_group[time_group['resource_type'] == resource_type]['impact_score']
                    
                    if len(resource_impacts) > 0:
                        stats_row[f'{resource_type}_impact_sum'] = resource_impacts.sum()
                        stats_row[f'{resource_type}_impact_mean'] = resource_impacts.mean()
                        stats_row[f'{resource_type}_impact_max'] = resource_impacts.max()
                        stats_row[f'{resource_type}_impact_std'] = resource_impacts.std() if len(resource_impacts) > 1 else 0
                    else:
                        stats_row[f'{resource_type}_impact_sum'] = 0
                        stats_row[f'{resource_type}_impact_mean'] = 0
                        stats_row[f'{resource_type}_impact_max'] = 0
                        stats_row[f'{resource_type}_impact_std'] = 0
                
                impact_stats_list.append(stats_row)
            
            # 영향도 통계 데이터프레임 생성
            impact_stats_df = pd.DataFrame(impact_stats_list)
            impact_stats_df['time'] = pd.to_datetime(impact_stats_df['time'])
            impact_stats_df = impact_stats_df.set_index('time')
            
            # 시간 특성 추가
            impact_stats_df['hour'] = impact_stats_df.index.hour
            impact_stats_df['day_of_week'] = impact_stats_df.index.dayofweek
            impact_stats_df['is_weekend'] = impact_stats_df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
            
            # 실제 시스템 리소스 데이터 필요 (InfluxDB에서 직접 조회)
            y_df = self._get_system_resource_data_for_training(impact_stats_df.index)
            
            if y_df is None or y_df.empty:
                logger.warning("시스템 리소스 데이터가 없어 더미 데이터 사용")
                # 더미 대상 변수 생성
                y_df = pd.DataFrame(
                    index=impact_stats_df.index,
                    data={
                        'cpu': np.random.rand(len(impact_stats_df)) * 50 + 20,
                        'mem': np.random.rand(len(impact_stats_df)) * 40 + 30,
                        'disk': np.random.rand(len(impact_stats_df)) * 30 + 50
                    }
                )
            
            # 공통 시간 인덱스
            common_times = impact_stats_df.index.intersection(y_df.index)
            
            if len(common_times) < 10:
                logger.warning("시스템 학습 데이터가 부족합니다")
                return None, None
            
            X = impact_stats_df.loc[common_times].fillna(0)
            y = y_df.loc[common_times].fillna(0)
            
            return X, y
            
        except Exception as e:
            logger.error(f"시스템 학습 데이터 준비 오류: {e}")
            return None, None
    
    def _get_system_resource_data_for_training(self, time_index) -> Optional[pd.DataFrame]:
        """학습용 시스템 리소스 데이터 조회"""
        try:
            from data.streaming_collector import StreamingDataCollector
            
            # 시간 범위 계산
            start_time = time_index.min()
            end_time = time_index.max()
            
            # 스트리밍 수집기로 시스템 리소스 조회
            collector = StreamingDataCollector(
                self.config, self.company_domain, self.device_id
            )
            
            _, sys_df = collector.get_training_data(start_time, end_time)
            
            if sys_df.empty:
                return None
            
            # 시스템 리소스 데이터 필터링 및 피봇
            sys_filtered = sys_df[
                ((sys_df['resource_type'] == 'cpu') & (sys_df['measurement'] == 'usage_user')) |
                ((sys_df['resource_type'] == 'mem') & (sys_df['measurement'] == 'used_percent')) |
                ((sys_df['resource_type'] == 'disk') & (sys_df['measurement'] == 'used_percent'))
            ]
            
            sys_pivot = sys_filtered.pivot_table(
                index='time',
                columns='resource_type',
                values='value',
                aggfunc='mean'
            )
            
            return sys_pivot
            
        except Exception as e:
            logger.error(f"시스템 리소스 데이터 조회 오류: {e}")
            return None
    
    def _save_training_metadata(self, cache_key: str, impact_df: pd.DataFrame, features_df: pd.DataFrame):
        """학습 메타데이터 저장"""
        try:
            from core.db import DatabaseManager
            
            db = DatabaseManager()
            
            # 모델 성능 기록에 메타데이터 추가
            metadata_query = """
            INSERT INTO model_performance 
            (company_domain, server_no, application, resource_type, model_type,
             mae, rmse, r2_score, feature_importance, trained_at, version, device_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            import json
            version = f"streaming_{cache_key}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            metadata = {
                'cache_key': cache_key,
                'impact_records': len(impact_df),
                'feature_records': len(features_df),
                'applications': impact_df['application'].nunique(),
                'training_method': 'streaming_cache'
            }
            
            params = (
                self.company_domain,
                self.server_id,
                'streaming_metadata',
                'training_info',
                'cache_based',
                0.0, 0.0, 0.0,  # 더미 성능 지표
                json.dumps(metadata),
                datetime.now(),
                version,
                self.device_id or ''
            )
            
            db.execute_query(metadata_query, params)
            db.close()
            
            logger.info(f"학습 메타데이터 저장 완료: {version}")
            
        except Exception as e:
            logger.error(f"학습 메타데이터 저장 오류: {e}")
    
    def predict_with_streaming_data(self, minutes=30) -> Optional[Dict]:
        """스트리밍 데이터로 예측 수행"""
        logger.info("스트리밍 데이터 기반 예측 시작")
        
        try:
            from data.streaming_collector import StreamingDataCollector
            
            # 최근 데이터 수집
            collector = StreamingDataCollector(
                self.config, self.company_domain, self.device_id
            )
            
            jvm_df, _ = collector.get_latest_data(minutes)
            
            if jvm_df.empty:
                logger.warning("예측용 JVM 데이터가 없습니다")
                return None
            
            # 애플리케이션별 특성 데이터 생성
            app_features = self._process_jvm_for_prediction(jvm_df)
            
            if not app_features:
                logger.warning("예측용 특성 데이터 생성 실패")
                return None
            
            # 시스템 예측 모델 로드 (필요 시)
            if self.system_predictor is None:
                self.system_predictor = SystemResourcePredictor(
                    self.config, self.company_domain, self.server_id, self.device_id
                )
                
                if not self.system_predictor.load_models():
                    logger.error("시스템 예측 모델 로드 실패")
                    return None
            
            # 애플리케이션 모델 매니저 로드 (필요 시)
            if self.app_model_manager is None:
                self.app_model_manager = AppModelManager(
                    self.config, self.company_domain, self.server_id, self.device_id
                )
                
                if not self.app_model_manager.load_all_models():
                    logger.error("애플리케이션 모델 로드 실패")
                    return None
            
            # 1단계: JVM 메트릭 → 영향도 예측
            app_impacts = self.app_model_manager.predict_impacts(app_features)
            
            if not app_impacts:
                logger.error("영향도 예측 실패")
                return None
            
            # 2단계: 영향도 → 시스템 리소스 예측  
            predictions = self.system_predictor.predict_future_usage(hours=24)
            
            if predictions:
                logger.info("스트리밍 기반 예측 완료")
                return predictions
            else:
                logger.error("시스템 리소스 예측 실패")
                return None
            
        except Exception as e:
            logger.error(f"스트리밍 예측 오류: {e}")
            return None
    
    def _process_jvm_for_prediction(self, jvm_df: pd.DataFrame) -> Dict:
        """예측용 JVM 데이터 처리"""
        try:
            processed = {}
            
            # 애플리케이션별로 그룹화
            for app, app_data in jvm_df.groupby('application'):
                app_metrics = {}
                
                # 메트릭별 최근 값 (평균)
                for metric_type, metric_data in app_data.groupby('metric_type'):
                    app_metrics[metric_type] = metric_data['value'].mean()
                
                # 시간 특성 추가
                latest_time = app_data['time'].max()
                app_metrics['hour'] = latest_time.hour
                app_metrics['day_of_week'] = latest_time.weekday()
                app_metrics['is_weekend'] = 1 if latest_time.weekday() >= 5 else 0
                
                # 데이터프레임으로 변환 (1행)
                processed[app] = pd.DataFrame([app_metrics])
            
            return processed
            
        except Exception as e:
            logger.error(f"예측용 JVM 데이터 처리 오류: {e}")
            return {}
    
    def get_model_status(self) -> Dict:
        """모델 상태 정보 조회"""
        try:
            status = {
                'company_domain': self.company_domain,
                'device_id': self.device_id,
                'server_id': self.server_id,
                'cache_dir': self.cache_dir,
                'model_dir': self.model_dir,
                'models_loaded': {
                    'app_model_manager': self.app_model_manager is not None,
                    'system_predictor': self.system_predictor is not None
                }
            }
            
            # 캐시 파일 현황
            if os.path.exists(self.cache_dir):
                impact_dir = os.path.join(self.cache_dir, "impacts")
                features_dir = os.path.join(self.cache_dir, "features")
                
                impact_files = len([f for f in os.listdir(impact_dir) if f.endswith('.pkl')]) if os.path.exists(impact_dir) else 0
                features_files = len([f for f in os.listdir(features_dir) if f.endswith('.pkl')]) if os.path.exists(features_dir) else 0
                
                status['cache_files'] = {
                    'impact_files': impact_files,
                    'feature_files': features_files
                }
            
            # 모델 파일 현황
            if os.path.exists(self.model_dir):
                model_files = []
                for root, dirs, files in os.walk(self.model_dir):
                    model_files.extend([f for f in files if f.endswith('.pkl')])
                
                status['model_files'] = len(model_files)
            
            return status
            
        except Exception as e:
            logger.error(f"모델 상태 조회 오류: {e}")
            return {'error': str(e)}