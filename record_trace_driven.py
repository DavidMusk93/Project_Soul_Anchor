from soul_anchor.manager import MemoryManager

def main():
    db_path = "aime_evolution.duckdb"
    print(f"Connecting to {db_path}...")
    
    # 按照之前的踩坑经验，对于已有 v1.5.0 的物理库，可以直接连接了
    # 这里我们正常连接即可，如果报 variant 错，就在脚本里用 attach 绕过
    mm = MemoryManager(db_path)
    mm.connect()
    
    payload = {
        "user_id": "sunmingqiang",
        "knowledge_type": "workflow_principle",
        "title": "追踪驱动与日志自理 (Trace-Driven)",
        "canonical_text": "在复杂代码修改或执行过程中，Agent 必须主动设计追溯日志作为‘思维的面包屑’。明确记录自己的意图、关键变量状态，而不仅仅是报错。当连续受挫时，强制打断惯性，先读取日志追溯因果，绝不盲目打补丁陷入死循环；当功能闭环、验证稳定后，主动清理冗余的 Debug 日志，保持代码纯洁。",
        "keywords": "trace-driven, log, death spiral, debug, clean code",
        "metadata": {"source": "record_trace_driven.py", "author": "Allen", "collaborator": "David"}
    }
    
    print("Saving Trace-Driven principle to L2 semantic_knowledge...")
    kid = mm.save_knowledge(payload)
    print(f"[SUCCESS] Principle saved with Knowledge ID: {kid}")
    
    print("\n--- Verifying Retrieval ---")
    results = mm.search_knowledge(user_id="sunmingqiang", query="追踪驱动与日志自理", top_k=1)
    for r in results:
        print(f"Title: {r.get('title')}")
        print(f"Content: {r.get('canonical_text')}")
        print(f"Keywords: {r.get('keywords')}")

if __name__ == "__main__":
    main()
