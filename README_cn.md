![CKibana Logo (Light)](img/logo-black.png#gh-light-mode-only)
![CKibana Logo (Dark)](img/logo-white.png#gh-dark-mode-only)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Release](https://img.shields.io/github/release/TongchengOpenSource/ckibana.svg?color=brightgreen&label=Release)](https://github.com/TongchengOpenSource/ckibana/releases)

---

- [CKibana最佳实践](https://mp.weixin.qq.com/s/T3tKYn6zE464bqTkoIa3dg)

## Introduce

CKibana: ClickHouse adapter for Kibana (ClickHouse proxy for kibana)

CKibana是一个为了能够在原生kibana上直接使用ElasticSearch语法查询ClickHouse的服务，可作为通用Clickhouse数据可视化工具。常见使用场景如:nginx日志从ElasticSearch迁移到ClickHouse后,不需要业务调整使用习惯就可以直接使用。

## Features

- 版本支持: 兼容ElasticSearch跟kibana 6.x、7.x、8.x 版本
- 语法支持: 兼容ElasticSearch常用语法(注: ip_range跟date_range仅可在搜索框中使用querystring语法查询)
- 采样功能: 对于命中结果超过阈值的查询,支持动态计算采样+还原结果,提高查询性能且保障图表趋势基本跟真实数据趋势一致(限流阈值越大跟真实图表趋势越接近)。
- 缓存功能: 支持使用ElasticSearch来做结果缓存,来提升重复查询的性能
- 时间Round功能: 支持round查询时间,比如20s的round(例子: 查询时间秒在0-19s则自动round到0,20s-39s自动round到20s,比如查询时间是从01:50:15到05:52:47,则自动调整为从01:
  50:00到05:52:40),配合缓存功能使用可以很好的缓解多个用户并发查询相同语句造成ClickHouse的压力
- 黑名单功能: 支持设置黑名单来防止复杂语句造成ClickHouse压力
- 查询模板功能: 支持按照去除时间后的语法监控,方便快速定位问题,配合黑名单功能一起使用可以很好的保障ClickHouse
- 查询熔断: 支持配置最大查询时间范围等高级功能

#### 支持的聚合语法：

| ES语法                  | 说明                     |
|-----------------------|------------------------|
| terms                   |                        |
| sum                     |                        |
| min                     |                        |
| max                    |                        |
| avg                     |                        |
| percentile_ranks         |                        |
| percentiles             |                        |
| filters                 | 目前仅支持第一层聚合             |
| filter item             |                        |
| cardinality             |                        |

## Get started
### quick start
要快速体验ckibana，可以通过docker-compose进行快速部署

[ckibana-quick-start文档](https://github.com/TongchengOpenSource/ckibana/blob/main/docker-compose/README.md)

```shell
# 进入docker-compose目录
cd ckibana/docker-compose
# 部署
docker-compose up -d
```
部署完成后，在浏览器访问kibana：http://127.0.0.1:5601/

已内置了mock数据的流程，导入kibana配置文件（大盘、index-pattern等），就可以开始体验了！

配置文件路径：docker-compose/quickstart-export.json
![](docker-compose/image/dashboard-import.jpg)
效果展示：
![](docker-compose/image/dashboard.jpg)



### 本地运行 ckibana
ckibana可以在所有主要操作系统上运行，需要安装Java JDK版本17或更高版本。要检查，请运行
`java -version`:
```shell
$ java -version
java version "17.0.5" 
```
代理服务依赖**ES、CK、Kibana**服务，需要提前准备好。以下只包含从ck查询数据步骤，不包含写数据到ck的步骤。

**1）建库、建表**

在ck中初始化库、表结构，可以参考[api-docs](https://tongchengopensource.github.io/ckibana-docs/zh/api-docs) 建表详解部分

**2）启动ckibana**

修改ckibana中的ES配置，需要改为自身的ES信息，配置文件路径为`src/main/resources/application.yml`
```yaml
metadata-config:
  hosts: your es metadata cluster hosts
  headers:
    headerKey: yourHeaderValue
```
打包运行 或者 本地运行直接com.ly.ckibana.Bootstrap类即可
```shell
### start ckibana
$ nohup java -jar ckibana.jar > run.out 2>&1 &

### check whether ckibana is successfully started
$ tail -f ~/logs/app.log
Tomcat started on port(s): 8080 (http) with context path ''
Started Bootstrap in 1.474 seconds
```

**3）启动kibana**

kibana的elasticsearchHosts参数配置为ckibana代理地址，这样就能走到代理服务中来
```shell
eg：elasticsearchHosts=http://ip:port
```

**4）配置index pattern白名单**

配置了对应的白名单，才可以在下一步创建index pattern，**配置的白名单需要跟表名一致**，可以参考[api-docs](https://tongchengopensource.github.io/ckibana-docs/zh/api-docs) 更新白名单索引接口

**5）在kibana中创建index pattern**

在kibana页面创建index pattern，名字跟1）的表名一致，且4）中配置白名单，然后就可以在kibana中进行查询了
```shell
eg：如果ck表名是table_test，则创建index pattern的名字就是table_test
```

以上5步都操作完以后，就可以创建大盘，开始进行使用了



## TODO

- 分段查询: 支持自动拆分查询时间,可以充分利用缓存,提升查询性能

## 文档地址
[CKibane-docs](https://tongchengopensource.github.io/ckibana-docs)  

## Contact

愿意参与构建CKibana或者是需要交流问题可以加入微信群(企业版微信和个人版本微信均可)
![](http://oss.17usoft.com/infra-github/ckibana/join-us.png)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
