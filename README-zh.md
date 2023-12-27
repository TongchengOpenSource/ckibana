# CKibana

## Introduce

CKibana: ClickHouse adapter for Kibana (ClickHouse proxy for kibana)

CKibana是一个为了能够在原生kibana上直接使用ElasticSearch语法查询ClickHouse的服务。常见使用场景如:nginx日志从ElasticSearch迁移到ClickHouse后,不需要业务调整使用习惯就可以直接使用。

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

## TODO

- 分段查询: 支持自动拆分查询时间,可以充分利用缓存,提升查询性能

## 文档地址
[CKibane-docs](https://github.com/TongchengOpenSource/ckibana-docs)  

## Contact

愿意参与构建CKibana或者是需要交流问题可以加入qq群
![](img/readme01.jpg)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
