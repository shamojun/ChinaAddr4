# 四级地址纠错

## 背景

项目里需要地址纠错功能，希望以**免费、低成本、快速响应**的方式解决用户输入地址的智能纠错问题。

## 问题

用户经常输入错别字，或者同音但拼写不同的街道名，导致整条地址都错。试过一些免费提供商的地址 API 纠错，效果不理想。

## 方案

基于开源地址数据集，自己实现**标准的四级（省/市/区/街道）地址纠错**能力。目标是把非标准地址纠正并匹配到**省/市/区（县）/街道（四级）**粒度，提升解析准确性与可用性。

## 整体架构

1. **数据层**：基于行政区划数据集构建四级地址基础库（省/市/区/街道），并支持别名扩展。
2. **索引层**：通过 uild_index.py 构建 SQLite FTS5 索引，用于高性能模糊检索。
3. **匹配引擎**：ddress_matcher.py 负责地址纠错、候选召回与评分排序。
4. **服务层**：pp.py 提供 HTTP API，对外提供 /match 查询服务。

## 进一步方案

1. 完善数据集到 5 级地址（街道 / 小区 / 村庄信息），目前仅覆盖行政区域信息。
2. 使用向量相似检索提高匹配精度，当前采用 SQLite FTS5 做关键词检索。

## 工程文件说明

运行只依赖 `dist/data.sqlite`。

- pp.py：HTTP 服务入口，对外提供 /match 查询接口
- ddress_matcher.py：地址纠错与匹配核心逻辑
- uild_index.py：构建 SQLite FTS5 离线索引
- dist/：行政区划数据集与别名数据（详见致谢）
- equirements.txt：Python 依赖列表

## 使用

### 第一步：处理离线数据（可多次清理）

`ash
python build_index.py
`

> 如需重新生成索引，可先清理旧的索引文件后再执行（可重复多次）。

### 第二步：启动服务并查询

`ash
python app.py
`

接口示例：

`ash
GET /match?q=地址文本&deep=1&topn=5
POST /match {"address":"地址文本","deep":true,"topn":5}
`

## 致谢

感谢开源项目 **Administrative-divisions-of-China** 提供高质量行政区划数据集，为本项目的地址纠错与标准化奠定了基础。

特别说明：dist 目录下的数据集由 **Administrative-divisions-of-China** 提供，来源地址：[Administrative-divisions-of-China](https://github.com/modood/administrative-divisions-of-china)。再次感谢该项目的开源贡献。
