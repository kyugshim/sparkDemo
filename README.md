# 요구사항

---

아래 사용자 activity 로그를 Hive table 로 제공하기 위한 Spark Application 을 작성하세요

https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

2019-Nov.csv,  2019-Oct.csv

- [ ] enable HIVE
- [ ] KST 기준 daily partition 처리
- [ ] 재처리 후 parquet, snappy 처리
- [ ] External Table 방식으로 설계
- [ ] 추가 기간 처리에 대응가능하도록 구현
- [ ] 배치 장애시 복구를 위한 장치 구현

Spark Application 구현에 사용가능한 언어는 **Scala나 Java**로 제한합니다.

File Path를 포함하여 제출시 특정할 수 없는 정보는 모두 임의로 작성해주세요.