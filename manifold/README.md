# Research Plan: User-Level Decision Modeling on Manifold

The goal is to create **per-user decision profiles** from Manifold.com and attempt to build **personalization models** for predicting user decisions.


## Data & Storage

Start with a **Postgres 3-table setup**:

- **Users table** – stores user metadata (id, username, created time, balance, etc.)
- **Decisions (Bets) table** – each bet placed by a user (amount, outcome, probability before/after, timestamp)
- **Markets table** – market questions, tags, outcome types, and answer options


## Experiment Design

Hold out a subset of user decisions as **ground truth test data**.  
Compare four modeling approaches on the held-out set:

1. **Baseline LLM (no context)**  
   Treats each decision as random; serves as a lower bound.

2. **LLM + Prompt Context**  
   Feed recent user decisions into the prompt and ask the model to predict the next choice.

3. **RAG with Prior Decisions**  
   Embed user’s historical decisions, retrieve the most relevant ones for a new market, and provide them as context to the LLM.

4. **PEFT/LoRA Per-User Adapter**  
   Train lightweight fine-tuning adapters for each user to personalize the LLM directly.

---
