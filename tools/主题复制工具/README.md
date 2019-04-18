# 主题复制脚本说明
## 自研kafka->ckafka 主题复制
### 使用场景
开源kafka->ckafka迁移，通过脚本create_topic.py 可以读取开源kafka主题名称，副本数，分区数，并批量复制到指定的ckafka 实例中，具体操作说明可以参考create_topic.py脚本中注释说明。
## ckafka->ckafka 主题复制
### 使用场景
ckafka->ckafka主题迁移，通过脚本copy_topic.py, 复制ckafka主题并批量复制到指定的ckafka 实例中，具体操作说明可以参考copy_topic.py脚本中注释说明。
