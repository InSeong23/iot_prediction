#!/bin/bash

# 환경변수 확인
echo "JVM 예측 시스템 시작"
echo "MySQL 호스트: $MYSQL_HOST"
echo "InfluxDB URL: $INFLUXDB_URL"

# 데이터베이스 연결 대기
echo "데이터베이스 연결 확인 중..."
for i in {1..30}; do
    python -c "from core.db import DatabaseManager; db = DatabaseManager(); exit(0 if db.connection else 1)" && break
    echo "데이터베이스 연결 대기 중... ($i/30)"
    sleep 2
done

# 초기 설정 실행 (테이블 생성)
echo "데이터베이스 초기 설정 확인"
python scripts/setup_database.py

# 데이터 결측 확인 및 처리
echo "데이터 상태 확인"
python -c "
from core.db import DatabaseManager
from datetime import datetime, timedelta
db = DatabaseManager()

# 최근 데이터 확인
query = '''
SELECT 
    COUNT(*) as count,
    MAX(time) as latest_time
FROM system_resources
WHERE time > DATE_SUB(NOW(), INTERVAL 1 DAY)
'''
result = db.fetch_one(query)
if result and result[0] > 0:
    print(f'최근 24시간 데이터: {result[0]}개, 최신: {result[1]}')
else:
    print('최근 데이터 없음. 초기 수집이 필요합니다.')
"

# 메인 애플리케이션 실행
exec python main.py --mode scheduler