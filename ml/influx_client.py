from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
import pandas as pd
from core.logger import logger

class InfluxDBManager:
    def __init__(self, config_manager):
        self.config = config_manager
        influx_config = self.config.get_influxdb_config()
        
        self.client = InfluxDBClient(
            url=influx_config['url'],
            token=influx_config['token'],
            org=influx_config['org']
        )
        self.query_api = self.client.query_api()
        
    def get_system_metrics(self, start_time, end_time=None, resource_type="cpu"):
        """시스템 리소스 메트릭 조회"""
        if end_time is None:
            end_time = datetime.utcnow()
            
        query = f'''
        from(bucket: "{self.config.get('influxdb_bucket')}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => r["companyDomain"] == "{self.config.get('company_domain')}")
        |> filter(fn: (r) => r["gatewayId"] == "{resource_type}")
        '''
        
        try:
            result = self.query_api.query_data_frame(query)
            return pd.DataFrame(result)
        except Exception as e:
            logger.error(f"InfluxDB 시스템 메트릭 조회 오류: {e}")
            return pd.DataFrame()

    def get_jvm_metrics(self, start_time, end_time=None, application=None):
        """JVM 메트릭 조회"""
        # ... JVM 메트릭 조회 로직 구현
