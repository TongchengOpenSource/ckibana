# docker-compose for ckibana

## 简介

使用docker-compose部署ckibana及关联服务，便于快速体验。

内置了mock数据的流程：nginx生产日志，filebeat抓取日志并写入clickhouse中。


## 使用
```shell
# 进入docker-compose目录
cd ckibana/docker-compose

# 部署
docker-compose up -d

# 卸载
docker-compose down
```
部署完成后，可在浏览器访问kibana：http://127.0.0.1:5601/


## 导入kibana配置文件
### 导入配置：
已内置了mock数据的流程，导入kibana配置文件（大盘、index-pattern等），就可以开始体验了！

配置文件路径：docker-compose/quickstart-export.json
![](image/dashboard-import.jpg)

### 效果展示：
![](image/dashboard.jpg)

