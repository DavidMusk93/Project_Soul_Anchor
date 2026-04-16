# Embedding 规划（Phase 3.4）

本文档用于规划 “embedding（向量）” 在 Project Soul Anchor 中的定位、对外接口策略、落地路径与使用场景 demo。

## 1. 目标与边界

### 1.1 Embedding 要解决的问题

- **召回**：关键词检索命不中（同义改写/中英混合/表达风格变化）时仍能召回相关记忆。
- **去重/合并**：候选知识（`knowledge_candidate`）升级、重复检测从“精确相等”升级为“相似度判断 + 可解释依据”。
- **冲突识别**：同主题/同标题但语义不一致的候选更可靠地进入 `conflict_registry`，而不是误合并。
- **上下文组装**：从“最近/显著”变成“相关 + 最近 + 显著”的混排，减少无关 L1 噪声。

### 1.2 非目标（首轮不做）

- 不在首轮引入复杂 ANN 索引（例如 DuckDB VSS 扩展、Faiss、Milvus）。
- 不做多模型路由/动态选择 embedding 模型。
- 不做对外 SLA 的 embedding 服务（先把本地链路跑通、可审计、可回滚）。

## 2. Embedding 的定位：内部使用 vs 对外暴露

这里有两个方向，建议从 **内部使用为主** 起步，逐步演进为“可选对外暴露”。

### 2.1 方案 A：Embedding 仅内部使用（推荐默认）

**定义**：embedding 只作为 Soul Anchor 内部检索与门控的信号，上层/外部只看到 “更好的召回结果”，不直接拿到向量。

**优点**

- 安全：避免向量泄露带来的隐私/推断风险（向量本身可能暴露语义信息）。
- 易演进：后续替换 embedding 模型/维度/归一化策略不会影响外部调用方。
- 易运维：无需对外承诺向量维度、数值稳定性、兼容性。

**缺点**

- 外部系统无法复用向量做自己的向量库/跨系统检索。

**适用场景**

- Soul Anchor 作为 “记忆系统” 被上层 Agent 调用，外部只关心召回质量与可解释审计。

### 2.2 方案 B：Embedding 通过 API 对外暴露（可选高级功能）

**定义**：对外提供 “获取/写入 embedding” 的接口，允许外部系统把向量同步到自己的索引或做二次检索。

**优点**

- 外部系统可做跨系统向量检索、统一向量库、离线分析、AB 测试等。
- 可用于在外部构建更强的检索服务（如果未来迁移到 VSS/ANN）。

**缺点 / 风险**

- **契约负担**：必须承诺 embedding 维度、模型版本、归一化策略、兼容策略。
- **安全合规**：向量可能携带敏感语义，需要权限/脱敏策略。
- **耦合**：一旦外部依赖 embedding 维度与分布，内部换模型成本大。

**适用场景**

- Soul Anchor 被多个系统共享，需要把向量同步到统一检索基础设施。

### 2.3 推荐策略：内部默认 + 可控对外（版本化）

推荐落地策略：

1. **默认不暴露向量**：所有检索接口只返回文本与解释性字段。
2. **在内部保存 embedding_version**：写入 `metadata` 或新增字段（后续 schema 扩展）记录模型版本。
3. 若要对外暴露：提供可选字段 `include_embedding=true`，并附带 `embedding_model_id`，同时做权限控制。

## 3. 数据模型与存储（现状与扩展）

### 3.1 现状（已存在）

- `context_stream.embedding FLOAT[]`
- `semantic_knowledge.embedding FLOAT[]`

目前 embedding 仅可存储/读回，检索尚未使用向量相似度。

### 3.2 建议新增/复用的元信息

建议先复用 `metadata VARIANT` 存储 embedding 元信息（无需改 schema）：

- `metadata.embedding_model`: `"text-embedding-3-large"` / `"internal:v1"` / `"dummy:v0"`
- `metadata.embedding_dim`: 1536 / 1024 / ...
- `metadata.embedding_norm`: `"l2"` / `"none"`
- `metadata.embedding_created_at`: timestamp

## 4. 落地路线（分阶段）

### 4.1 Phase 3.4.1：写入链路（embedding 自动生成）

目标：让 embedding 真正“出现在数据里”，并有可控开关。

- 在 `save_episode` / `save_knowledge`：
  - 如果调用方未提供 `embedding`，则内部调用 `Embedder` 生成并写入。
  - 如果调用方提供了 `embedding`，直接写入（用于外部预计算）。
- `Embedder` 设计：`embed_text(text) -> list[float]`，支持 `model_id`。
- 失败策略：embedding 生成失败不应阻塞写入（降级写入 embedding=None，并写审计/日志）。

### 4.2 Phase 3.4.2：检索链路（Hybrid 召回与排序）

目标：在不引入 ANN 的前提下，完成 “关键词 + 向量” 的混排。

建议策略：

1. 先用现有关键词检索取一个候选集（例如 top 100）。
2. 对候选集在 Python 侧做 cosine 相似度计算，得到 `vector_score`。
3. 最终排序：`hybrid_score = w_vec*vector + w_kw*keyword + w_stability*stability + w_recency*recency`
4. 返回可解释字段：
   - `vector_score`
   - `hybrid_score`
   - `match_reasons`（继续保留）

### 4.3 Phase 3.4.3：门控升级（重复/冲突从精确匹配到相似度）

目标：CandidateProcessor 与 Gating 增强：

- duplicate：`cosine >= threshold`（例如 0.97）视为重复（阈值可配置）。
- conflict：同 title/type，但 `cosine <= threshold_low`（例如 0.85）更倾向冲突。
- 在 `candidate_payload` 里写入：
  - `similarity_to_existing`
  - `duplicate_threshold` / `conflict_threshold`

### 4.4 Phase 3.4.4：性能与索引（可选）

当数据量上来（例如 L2 > 50k）才考虑：

- DuckDB VSS 扩展 / FTS + VSS
- 外部 ANN（Faiss/Milvus）并通过 “可选对外暴露” 的 embedding API 同步

## 5. 对外接口设计（建议）

### 5.1 内部接口（推荐首选）

- `search_knowledge(user_id, query, top_k, use_embedding=False)`
- `search_context(session_id, user_id, query, top_k, use_embedding=False)`

当 `use_embedding=True` 时，返回额外字段：

- `vector_score`
- `hybrid_score`
- `embedding_model`（从 metadata 或配置拿）

默认 `use_embedding=False`，避免首轮引入成本与不可控回归。

### 5.2 对外暴露（可选）

仅当确有外部系统需要时，考虑增加：

- `get_knowledge_embedding(knowledge_id, include_vector=False)`
- `upsert_knowledge_embedding(knowledge_id, embedding, embedding_model_id)`

并明确：

- 权限控制（用户/系统级）
- 模型版本固定或至少可查询
- 兼容策略（同一个 knowledge_id 允许多版本 embedding？）

## 6. 使用场景与 Demo

本节给出 “内部使用” 与 “对外暴露” 两种 demo。

### 6.1 场景 1：同义改写检索（内部使用 embedding）

用户说：

- 之前：`每次改动后都要提交，并且必须带 detail。`
- 现在：`提交要短，但说明要完整。`

仅靠关键词，可能命中不稳定；embedding 可提升命中率。

```python
from soul_anchor.manager import MemoryManager

mm = MemoryManager("aime_evolution.duckdb")
mm.connect()

# 写入一条知识（未来由内部自动生成 embedding）
kid = mm.save_knowledge(
    {
        "user_id": "u1",
        "knowledge_type": "workflow",
        "title": "Commit Discipline",
        "canonical_text": "每次改动后都要提交，并且必须带 detail。",
        "metadata": {"source": "user_preference"},
    }
)

# 未来实现：use_embedding=True 走混排召回
hits = mm.search_knowledge(user_id="u1", query="提交要短，但说明要完整", top_k=5)
print(hits[0]["title"])
```

### 6.2 场景 2：候选去重（CandidateProcessor + embedding）

当用户多次用不同表达描述同一规则：

- “每次提交都要有 detail”
- “commit 信息要包含变更原因和验证方式”

Phase 3.4.3 后，候选处理应能判定为 “语义重复” 而不是生成大量相近条目。

```python
from soul_anchor.manager import MemoryManager
from soul_anchor.agentic.tools import MemoryToolAPI
from soul_anchor.agentic.candidates import CandidateProcessor

mm = MemoryManager("aime_evolution.duckdb")
mm.connect()
tools = MemoryToolAPI(mm)

cid = tools.save_knowledge_candidate(
    {
        "user_id": "u1",
        "knowledge_type": "workflow",
        "title": "Commit Discipline",
        "canonical_text": "commit 信息要包含变更原因和验证方式",
        "candidate_payload": {"source": "chat"},
    }
)

processor = CandidateProcessor(mm)
stats = processor.process_pending(limit=10)
print(stats)
```

### 6.3 场景 3：对外暴露 embedding（可选）

如果外部需要把向量同步到统一向量库：

```python
# 伪代码：对外 API 可能长这样（需要权限控制与版本化）
embedding = api.get_knowledge_embedding(knowledge_id=42, include_vector=True)
external_vector_store.upsert(id=42, vector=embedding["vector"], metadata=embedding["meta"])
```

## 7. 可观测性与运维

### 7.1 审计

embedding 相关操作必须写审计（建议 action_type）：

- `embed_episode`
- `embed_knowledge`
- `hybrid_search_knowledge`
- `hybrid_search_context`
- `candidate_similarity_check`

写入 `memory_audit_log` 的 `tool_payload` 至少包含：

- `embedding_model`
- `embedding_dim`
- `use_embedding`
- `thresholds`（若涉及去重/冲突）

### 7.2 失败降级

embedding 服务不可用时：

- 写入：允许 embedding 为空（不阻塞核心写入）
- 检索：自动退回关键词检索（并写审计 `result_summary="fallback"`）

## 8. 需要你确认的决策（落地前置条件）

1. 首轮 embedding 模型来源：
   - 本地 dummy（只为走通链路，效果不保证）
   - 内部/外部 embedding 服务（效果最好，但需要依赖配置与鉴权）
2. 首轮覆盖范围：
   - 仅 L2（推荐先做）
   - L1 + L2（成本更高，但上下文相关性提升更明显）

