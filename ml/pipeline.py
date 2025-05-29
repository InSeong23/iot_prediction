from datetime import datetime, timedelta
import os
import joblib
import numpy as np
from influx_client import InfluxDBManager
from core.logger import logger
from core.db import DatabaseManager

class MLPipeline:
    def __init__(self, config_manager):
        self.config = config_manager
        self.influx = InfluxDBManager(config_manager)
        self.model_dir = f"models/{self.config.get('company_domain')}/{self.config.get('server_id')}"
        os.makedirs(self.model_dir, exist_ok=True)
        
    def prepare_training_data(self, days=3):
        """InfluxDB에서 학습 데이터 준비"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)
        
        # 시스템 리소스 데이터
        system_data = self.influx.get_system_metrics(start_time, end_time)
        
        # JVM 메트릭 데이터
        jvm_data = self.influx.get_jvm_metrics(start_time, end_time)
        
        return self._preprocess_data(system_data, jvm_data)
        
    def train_model(self, X, y, resource_type):
        """모델 학습"""
        # ... 모델 학습 로직 구현

    def _save_model_metadata(self, model_path, version):
        """모델 메타데이터 저장"""
        db = DatabaseManager()
        try:
            query = """
                INSERT INTO model_metadata 
                (company_domain, server_no, model_path, model_version, 
                training_start, training_end, metrics, is_active)
                VALUES (%s, %s, %s, %s, NOW(), NOW(), %s, TRUE)
            """
            metrics = {'accuracy': 0.95}  # 실제 메트릭으로 교체 필요
            db.execute_query(query, (
                self.config.get('company_domain'), self.config.get('server_id'), 
                model_path, version, str(metrics)
            ))
        finally:
            db.close()
