"""
스트리밍 전처리기 - 파일 기반 영향도/특성 캐싱
MySQL 저장 없이 계산 결과를 파일로 관리
"""
import os
import json
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from core.config_manager import ConfigManager
from core.logger import logger

class StreamingPreprocessor:
    """스트리밍 전처리기 - 파일 기반 캐싱"""
    
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
        
        # 기본 설정
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        
        # 캐시 디렉토리 설정
        self.cache_dir = os.path.join("cache", self.company_domain, self.device_id or "global")
        self.impact_dir = os.path.join(self.cache_dir, "impacts")
        self.features_dir = os.path.join(self.cache_dir, "features")
        
        os.makedirs(self.impact_dir, exist_ok=True)
        os.makedirs(self.features_dir, exist_ok=True)
        
        # 전처리 설정
        self.window_sizes = self.config.get('window_sizes', [5, 15, 30, 60])
        self.statistics = self.config.get('statistics', ['mean', 'std', 'max', 'min'])
        self.resample_interval = self.config.get('resample_interval', '5min')
        
        logger.info(f"스트리밍 전처리기 초기화: {self.company_domain}/{self.device_id}")
    
    # streaming_preprocessor.py 수정
    # calculate_and_cache_impacts 메서드에 cache_metadata 저장 추가

    def calculate_and_cache_impacts(self, jvm_df: pd.DataFrame, sys_df: pd.DataFrame, 
                                cache_key: str, force_recalculate=False) -> Optional[pd.DataFrame]:
        """영향도 계산 및 캐싱"""
        impact_file = os.path.join(self.impact_dir, f"impact_{cache_key}.pkl")
        
        # 캐시된 영향도가 있고 강제 재계산이 아니면 로드
        if not force_recalculate and os.path.exists(impact_file):
            try:
                with open(impact_file, 'rb') as f:
                    impact_df = pickle.load(f)
                logger.info(f"캐시된 영향도 사용: {cache_key}")
                return impact_df
            except Exception as e:
                logger.warning(f"영향도 캐시 로드 실패: {e}")
        
        # 영향도 계산
        logger.info(f"영향도 계산 시작: {cache_key}")
        impact_df = self._calculate_impact_scores(jvm_df, sys_df)
        
        if impact_df is not None and not impact_df.empty:
            # 캐시에 저장
            try:
                with open(impact_file, 'wb') as f:
                    pickle.dump(impact_df, f)
                
                # 메타데이터도 함께 저장
                meta_file = os.path.join(self.impact_dir, f"impact_{cache_key}_meta.json")
                metadata = {
                    'created_at': datetime.now().isoformat(),
                    'company_domain': self.company_domain,
                    'device_id': self.device_id,
                    'jvm_records': len(jvm_df),
                    'sys_records': len(sys_df),
                    'impact_records': len(impact_df)
                }
                
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                logger.info(f"영향도 캐시 저장 완료: {cache_key} ({len(impact_df)}개)")
                
            except Exception as e:
                logger.warning(f"영향도 캐시 저장 실패: {e}")
        
        return impact_df

    # generate_and_cache_features 메서드에도 동일하게 추가
    def generate_and_cache_features(self, jvm_df: pd.DataFrame, cache_key: str, 
                                force_recalculate=False) -> Optional[pd.DataFrame]:
        """특성 생성 및 캐싱"""
        features_file = os.path.join(self.features_dir, f"features_{cache_key}.pkl")
        
        # 캐시된 특성이 있고 강제 재계산이 아니면 로드
        if not force_recalculate and os.path.exists(features_file):
            try:
                with open(features_file, 'rb') as f:
                    features_df = pickle.load(f)
                logger.info(f"캐시된 특성 사용: {cache_key}")
                return features_df
            except Exception as e:
                logger.warning(f"특성 캐시 로드 실패: {e}")
        
        # 특성 생성
        logger.info(f"특성 생성 시작: {cache_key}")
        features_df = self._generate_features(jvm_df)
        
        if features_df is not None and not features_df.empty:
            # 캐시에 저장
            try:
                with open(features_file, 'wb') as f:
                    pickle.dump(features_df, f)
                
                # 메타데이터도 함께 저장
                meta_file = os.path.join(self.features_dir, f"features_{cache_key}_meta.json")
                metadata = {
                    'created_at': datetime.now().isoformat(),
                    'company_domain': self.company_domain,
                    'device_id': self.device_id,
                    'jvm_records': len(jvm_df),
                    'feature_records': len(features_df),
                    'unique_features': features_df['feature_name'].nunique() if 'feature_name' in features_df.columns else 0
                }
                
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                
                logger.info(f"특성 캐시 저장 완료: {cache_key} ({len(features_df)}개)")
                
            except Exception as e:
                logger.warning(f"특성 캐시 저장 실패: {e}")
        
        return features_df
    
    def generate_and_cache_features(self, jvm_df: pd.DataFrame, cache_key: str, 
                                   force_recalculate=False) -> Optional[pd.DataFrame]:
        """특성 생성 및 캐싱"""
        features_file = os.path.join(self.features_dir, f"features_{cache_key}.pkl")
        
        # 캐시된 특성이 있고 강제 재계산이 아니면 로드
        if not force_recalculate and os.path.exists(features_file):
            try:
                with open(features_file, 'rb') as f:
                    features_df = pickle.load(f)
                logger.info(f"캐시된 특성 사용: {cache_key}")
                return features_df
            except Exception as e:
                logger.warning(f"특성 캐시 로드 실패: {e}")
        
        # 특성 생성
        logger.info(f"특성 생성 시작: {cache_key}")
        features_df = self._generate_features(jvm_df)
        
        if features_df is not None and not features_df.empty:
            # 캐시에 저장
            try:
                with open(features_file, 'wb') as f:
                    pickle.dump(features_df, f)
                
                # 메타데이터도 함께 저장
                meta_file = os.path.join(self.features_dir, f"features_{cache_key}_meta.json")
                metadata = {
                    'created_at': datetime.now().isoformat(),
                    'company_domain': self.company_domain,
                    'device_id': self.device_id,
                    'jvm_records': len(jvm_df),
                    'feature_records': len(features_df),
                    'unique_features': features_df['feature_name'].nunique() if 'feature_name' in features_df.columns else 0
                }
                
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                logger.info(f"특성 캐시 저장 완료: {cache_key} ({len(features_df)}개)")
                
            except Exception as e:
                logger.warning(f"특성 캐시 저장 실패: {e}")
        
        return features_df
    
    def _calculate_impact_scores(self, jvm_df: pd.DataFrame, sys_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """영향도 점수 계산 (기존 로직 유지)"""
        if jvm_df.empty or sys_df.empty:
            logger.warning("영향도 계산을 위한 데이터가 충분하지 않음")
            return None
        
        try:
            # 시간 형식 검증 및 변환
            if not pd.api.types.is_datetime64_any_dtype(jvm_df['time']):
                jvm_df['time'] = pd.to_datetime(jvm_df['time'])
            
            if not pd.api.types.is_datetime64_any_dtype(sys_df['time']):
                sys_df['time'] = pd.to_datetime(sys_df['time'])
            
            # 시스템 리소스 데이터 필터링
            sys_filtered = sys_df[
                ((sys_df['resource_type'] == 'cpu') & (sys_df['measurement'] == 'usage_user')) |
                ((sys_df['resource_type'] == 'mem') & (sys_df['measurement'] == 'used_percent')) |
                ((sys_df['resource_type'] == 'disk') & (sys_df['measurement'] == 'used_percent'))
            ]
            
            if sys_filtered.empty:
                logger.error("필터링된 시스템 리소스 데이터가 없음")
                return None
            
            # 피봇 테이블 생성
            jvm_pivot = jvm_df.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'
            )
            
            sys_pivot = sys_filtered.pivot_table(
                index='time', 
                columns=['resource_type', 'measurement'], 
                values='value',
                aggfunc='mean'
            )
            
            if jvm_pivot.empty or sys_pivot.empty:
                logger.error("피봇 테이블 생성 실패")
                return None
            
            # 시계열 리샘플링 - 1분 단위로 변경
            jvm_resampled = jvm_pivot.resample('1min').mean().interpolate(method='linear', limit=5)
            sys_resampled = sys_pivot.resample('1min').mean().interpolate(method='linear', limit=5)
            
            # 공통 시간대 확인
            common_times = jvm_resampled.index.intersection(sys_resampled.index)
            
            logger.info(f"공통 시간대 수: {len(common_times)}")
            
            if len(common_times) < 5:
                logger.warning(f"공통 시간대가 너무 적음: {len(common_times)}개")
                # 원본 데이터로 다시 시도
                common_times = jvm_pivot.index.intersection(sys_pivot.index)
                if len(common_times) >= 5:
                    logger.info(f"원본 데이터 사용: {len(common_times)}개")
                    jvm_resampled = jvm_pivot
                    sys_resampled = sys_pivot
                else:
                    return None
            
            # 상관관계 계산
            impact_scores = self._compute_correlations(jvm_resampled, sys_resampled, common_times)
            
            if impact_scores:
                return pd.DataFrame(impact_scores)
            else:
                return None
                
        except Exception as e:
            logger.error(f"영향도 계산 중 오류: {e}")
            return None
    
    def _compute_correlations(self, jvm_resampled, sys_resampled, common_times):
        """상관관계 기반 영향도 계산 (기존 로직)"""
        jvm_aligned = jvm_resampled.loc[common_times]
        sys_aligned = sys_resampled.loc[common_times]
        
        impact_scores = []
        
        for resource_col in sys_aligned.columns:
            resource_type, measurement = resource_col
            sys_values = sys_aligned[resource_col]
            
            for app in jvm_aligned.columns.get_level_values(0).unique():
                app_metrics = [col for col in jvm_aligned.columns if col[0] == app]
                
                if not app_metrics:
                    continue
                
                corr_sum = 0
                corr_count = 0
                
                for app_metric in app_metrics:
                    jvm_values = jvm_aligned[app_metric]
                    
                    if jvm_values.isnull().all() or sys_values.isnull().all():
                        continue
                    
                    try:
                        corr = sys_values.corr(jvm_values)
                        if not np.isnan(corr):
                            corr_sum += abs(corr)
                            corr_count += 1
                    except Exception:
                        continue
                
                if corr_count > 0:
                    impact_score = corr_sum / corr_count
                    
                    for time_point in common_times:
                        impact_scores.append({
                            'time': time_point,
                            'application': app,
                            'resource_type': resource_type,
                            'impact_score': impact_score,
                            'calculation_method': 'correlation'
                        })
        
        return impact_scores
    
    def _generate_features(self, jvm_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """시계열 특성 생성 (기존 로직 유지)"""
        if jvm_df.empty:
            logger.warning("특성 생성을 위한 데이터가 없음")
            return None
        
        try:
            # 피봇 테이블로 변환
            pivot_df = jvm_df.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'
            )
            
            all_features = []
            applications = pivot_df.columns.get_level_values(0).unique()
            
            for app in applications:
                app_cols = [col for col in pivot_df.columns if col[0] == app]
                app_df = pivot_df[app_cols].copy()
                app_df.columns = [col[1] for col in app_df.columns]
                app_df = app_df.dropna()
                
                if app_df.empty:
                    continue
                
                # 윈도우별 특성 생성
                app_features = self._generate_app_features(app, app_df)
                all_features.extend(app_features)
            
            # 시간 특성 추가
            if self.config.get('time_features', True) and all_features:
                time_features = self._generate_time_features(applications, all_features)
                all_features.extend(time_features)
            
            if all_features:
                feature_df = pd.DataFrame(all_features)
                feature_df = feature_df.drop_duplicates(
                    subset=['time', 'application', 'feature_name'], 
                    keep='last'
                )
                return feature_df
            else:
                return None
                
        except Exception as e:
            logger.error(f"특성 생성 중 오류: {e}")
            return None
    
    def _generate_app_features(self, app, app_df):
        """애플리케이션별 특성 생성"""
        features = []
        
        for window_size in self.window_sizes:
            rolling_window = app_df.rolling(window=f"{window_size}min", min_periods=1)
            
            stats_dict = {}
            for stat in self.statistics:
                if stat == 'mean':
                    stats_dict[stat] = rolling_window.mean()
                elif stat == 'std':
                    stats_dict[stat] = rolling_window.std()
                elif stat == 'max':
                    stats_dict[stat] = rolling_window.max()
                elif stat == 'min':
                    stats_dict[stat] = rolling_window.min()
            
            for stat_name, stat_df in stats_dict.items():
                resampled = stat_df.resample(self.resample_interval).last().dropna()
                
                for metric in resampled.columns:
                    for timestamp, value in resampled[metric].items():
                        if not pd.isna(value):
                            features.append({
                                'time': timestamp,
                                'application': app,
                                'feature_name': f"{metric}_{stat_name}_{window_size}min",
                                'value': float(value),
                                'window_size': window_size
                            })
        
        return features
    
    def _generate_time_features(self, applications, all_features):
        """시간 특성 생성"""
        time_features = []
        unique_times = set(feature['time'] for feature in all_features)
        
        for app in applications:
            for time_point in unique_times:
                time_feature_list = [
                    ('hour', time_point.hour),
                    ('day_of_week', time_point.dayofweek),
                    ('is_weekend', 1 if time_point.dayofweek >= 5 else 0)
                ]
                
                for feature_name, value in time_feature_list:
                    time_features.append({
                        'time': time_point,
                        'application': app,
                        'feature_name': feature_name,
                        'value': float(value),
                        'window_size': 0
                    })
        
        return time_features
    
    def load_cached_impacts(self, cache_key: str) -> Optional[pd.DataFrame]:
        """캐시된 영향도 로드"""
        impact_file = os.path.join(self.impact_dir, f"impact_{cache_key}.pkl")
        
        if os.path.exists(impact_file):
            try:
                with open(impact_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"영향도 캐시 로드 오류: {e}")
        
        return None
    
    def load_cached_features(self, cache_key: str) -> Optional[pd.DataFrame]:
        """캐시된 특성 로드"""
        features_file = os.path.join(self.features_dir, f"features_{cache_key}.pkl")
        
        if os.path.exists(features_file):
            try:
                with open(features_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"특성 캐시 로드 오류: {e}")
        
        return None
    
    def cleanup_cache(self, max_age_hours=48):
        """오래된 캐시 정리"""
        logger.info(f"전처리 캐시 정리: {max_age_hours}시간 이상 파일 삭제")
        
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        deleted_count = 0
        
        for cache_subdir in [self.impact_dir, self.features_dir]:
            try:
                for filename in os.listdir(cache_subdir):
                    filepath = os.path.join(cache_subdir, filename)
                    
                    if os.path.isfile(filepath):
                        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                        
                        if file_time < cutoff_time:
                            os.remove(filepath)
                            deleted_count += 1
                            logger.debug(f"캐시 파일 삭제: {filename}")
                
            except Exception as e:
                logger.error(f"캐시 정리 오류 ({cache_subdir}): {e}")
        
        logger.info(f"전처리 캐시 정리 완료: {deleted_count}개 파일 삭제")
    
    def get_cache_status(self) -> Dict:
        """캐시 상태 조회"""
        try:
            impact_files = [f for f in os.listdir(self.impact_dir) if f.endswith('.pkl')]
            features_files = [f for f in os.listdir(self.features_dir) if f.endswith('.pkl')]
            
            impact_size = sum(os.path.getsize(os.path.join(self.impact_dir, f)) for f in impact_files)
            features_size = sum(os.path.getsize(os.path.join(self.features_dir, f)) for f in features_files)
            
            return {
                'cache_dir': self.cache_dir,
                'impacts': {
                    'file_count': len(impact_files),
                    'size_mb': round(impact_size / (1024 * 1024), 2),
                    'files': impact_files
                },
                'features': {
                    'file_count': len(features_files),
                    'size_mb': round(features_size / (1024 * 1024), 2),
                    'files': features_files
                },
                'total_size_mb': round((impact_size + features_size) / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"캐시 상태 조회 오류: {e}")
            return {'error': str(e)}