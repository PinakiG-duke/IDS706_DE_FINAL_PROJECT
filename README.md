# 🏗️ From Order to Insight: Scalable Data Engineering Pipeline for E-Commerce

**Course:** IDS 706 – Data Engineering  
**Instructor:** [Dr. Zhongyuan Yu]  
**Team:** [Pinaki Ghosh, Austin Zhang, Diwas Puri, Michael Badu]  
**Timeline:** Nov 12 → Dec 5 (Final Submission)

## 📘 Project Overview

This project designs and implements a **cloud-ready data engineering pipeline** using the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).  
It replicates a real-world enterprise analytics platform — automating data ingestion, transformation, and analysis — to produce reproducible, scalable insights about e-commerce operations, customer satisfaction, and logistics performance.

The final solution will combine **AWS cloud services, containerization, orchestration, and CI/CD automation** to demonstrate practical components of modern data engineering workflows.

## 🎯 Why This Dataset

Olist is a Brazilian marketplace that connects thousands of small sellers with customers nationwide.  
It was selected because:

- **Rich relational structure:** multiple linked tables across orders, customers, products, and logistics.
- **Realistic business process flow:** mirrors the operational data in real e-commerce pipelines.
- **Multi-domain scope:** enables demonstration of ingestion, transformation, analytics, and visualization within one integrated ecosystem.
- **Open, reproducible data:** no API keys or privacy restrictions — ideal for student projects and automation.

This dataset provides a controlled, yet sufficiently complex environment to demonstrate the entire data engineering lifecycle.

## Schema of the Dataset

<img width="2486" height="1496" alt="image" src="https://github.com/user-attachments/assets/c12594df-4515-45b9-93d4-1a4618ee0f76" />


## 🧩 Logical Architecture (with AWS Integration)

```mermaid
flowchart TD
    A[External Sources<br/>(Kaggle Dataset,<br/>Weather & Currency APIs)] -->|Extract via Python/Kaggle API| B[S3 Bucket<br/>(Raw Zone)]
    B -->|Metadata & Tracking| C[DynamoDB<br/>(Metadata Store)]
    B -->|Trigger ETL Job| D[AWS Glue / PySpark<br/>(Transformation Layer)]
    D -->|Clean, Join, Enrich| E[S3 Bucket<br/>(Processed Zone)]
    E -->|Load Curated Data| F[AWS RDS<br/>(PostgreSQL Warehouse)]
    F -->|Query & Analyze| G[Apache Airflow<br/>(Amazon MWAA)]
    G -->|Orchestrate Tasks & Logs| H[GitHub Actions<br/>(CI/CD Pipeline)]
    H -->|Build/Test/Deploy| I[Docker Containers<br/>(ECS/Local Compose)]
    F -->|Live Connection| J[Streamlit Dashboard<br/>(EC2 / AppRunner)]
    J -->|Visualization & KPIs| K[End Users / Analysts]

    subgraph AWS_Cloud [AWS Cloud]
        B
        C
        D
        E
        F
        G
        J
    end

    style A fill:#f2f2f2,stroke:#666,stroke-width:1px
    style B fill:#eaf4fe,stroke:#0073bb,stroke-width:1.5px
    style D fill:#e1f5e4,stroke:#3fa34d,stroke-width:1.5px
    style F fill:#fef7e0,stroke:#c48f00,stroke-width:1.5px
    style G fill:#f3e8ff,stroke:#7e57c2,stroke-width:1.5px
    style H fill:#e3f2fd,stroke:#0277bd,stroke-width:1.5px
    style I fill:#fce4ec,stroke:#c2185b,stroke-width:1.5px
    style J fill:#fff3e0,stroke:#ef6c00,stroke-width:1.5px
    style K fill:#eeeeee,stroke:#333,stroke-width:1px


## 🪜 Workflow & Learning Objectives-Planned

| Workflow Stage | Data-Engineering Concept | Expected Deliverable |
|----------------|--------------------------|----------------------|
| **Data Ingestion** | API and batch ingestion from Kaggle → S3 | Automated extraction + Airflow DAG |
| **Data Storage** | Raw → Processed → Analytical zoning | Normalized schema in AWS RDS |
| **Transformation** | Distributed compute, schema evolution | Polars/PySpark ETL jobs |
| **Orchestration** | Scheduling, retry logic, observability | Airflow DAGs with logging and SLA checks |
| **Testing & Validation** | Data quality and schema validation | Great Expectations reports |
| **CI/CD** | Automation and reproducibility | GitHub Actions + Docker build/test pipeline |
| **Analytics & Serving** | Querying, KPIs, and visualization | Streamlit dashboard with insights |
| **Governance & Security** | IAM, least-privilege access | Role-based permissions and audit logs |

## ⚙️ Proposed Tech Stack

| Layer | Tools |
|-------|-------|
| **Compute** | PySpark (Glue) / Polars |
| **Storage & Warehouse** | AWS S3 (Data Lake), AWS RDS (PostgreSQL) |
| **Ingestion** | Kaggle API, Requests, boto3 |
| **Orchestration** | Apache Airflow (Amazon MWAA) |
| **Testing** | Great Expectations, Pytest |
| **Containerization** | Docker, Docker Compose |
| **CI/CD** | GitHub Actions + AWS ECS |
| **Visualization** | Streamlit (EC2/AppRunner) |
| **Documentation** | Markdown, Quarto / Overleaf |


## 🔁 Reproducibility & Scalability

- **Reproducibility**
  - Environment codified via `Dockerfile` + `requirements.txt`
  - Consistent runs through GitHub Actions pipeline  
  - Versioned S3 storage for raw and processed data
  - All configs externalized (`.env`, YAMLs)

- **Scalability**
  - Modular DAGs and transformation scripts
  - Elastic AWS compute and storage (Glue, RDS)
  - Lazy evaluation and partitioned queries in Polars
  - Stateless containers ensure horizontal scalability
 
  ## 🗓️ Project Plan and Twice-a-Week Drops

| Drop | Date | Milestones | Deliverables |
|------|------|-------------|---------------|
| **Drop 1** | **Nov 14** | Environment setup, dataset profiling, schema draft | EDA notebook, draft schema diagram |
| **Drop 2** | **Nov 18** | Data ingestion pipeline + S3 load | Airflow DAG for ingestion, raw data storage validation |
| **Drop 3** | **Nov 21** | ETL transformation logic | PySpark/Polars scripts, processed zone creation |
| **Drop 4** | **Nov 25** | Analytical schema + queries | RDS schema + SQL transformations |
| **Drop 5** | **Nov 28** | Orchestration + CI/CD | Airflow orchestration, Docker + GitHub Actions |
| **Drop 6** | **Dec 2** | Dashboard + testing layer | Streamlit dashboard, Great Expectations suite |
| **Drop 7** | **Dec 5** | Final polish + dry run | Complete repo, video rehearsal, README finalization |
| **Submission** | **Dec 6** | Final delivery | Code + Docs + Video walkthrough |

All deliverables will be merged to `main` via reviewed pull requests with active commits twice a week from each member.

## 🧱 Development Best Practices to be followed

- **Branching:** Feature branches → Pull Request → Review → Merge  
- **Code Quality:** Black formatter + Pylint + docstrings  
- **Secrets:** Managed via AWS Secrets Manager, never hardcoded  
- **Testing:** Pytest + Great Expectations integrated into CI/CD  
- **Observability:** Airflow and CloudWatch monitoring, task-level logs  
- **Documentation:** Updated Markdown/Quarto after every drop  
- **Team Workflow:** Asynchronous commits; synchronous check-ins every drop day  
- **Peer Review:** Every merge request requires one documented reviewer approval

## 📦 Planned Deliverables

| Category | Deliverable | Description |
|-----------|--------------|-------------|
| **Repository** | Complete GitHub repo | Code, docs, notebooks, tests, Docker, CI/CD |
| **Cloud Deployment** | AWS-hosted pipeline | S3 + Glue + RDS + MWAA + EC2 integration |
| **Transformation Layer** | Polars / PySpark ETL | Data cleaning, joins, enrichments, metrics |
| **Analytics & Dashboard** | Streamlit app | Real-time KPIs, trend visuals, regression |
| **Testing & CI/CD** | Automated validation | Great Expectations + GitHub Actions |
| **Documentation** | PDF, diagrams, video | README, architecture, walkthrough demo |

## ⚠️ Risks and Contingencies

| Risk | Impact | Mitigation |
|------|---------|-------------|
| **AWS credential or quota limits** | Delayed deployment | Local MinIO + PostgreSQL mirror |
| **Glue or RDS compute limits** | ETL job failure or cost spikes | Process subset data locally, scale on need |
| **Scheduling conflicts** | Progress slippage | Dual ownership per module, fixed drop cadence |
| **Airflow DAG failures** | Pipeline disruption | Local Airflow container fallback |
| **Data quality inconsistencies** | Analysis bias | Early validation using Great Expectations |
| **Cost escalation** | Budget breach | Limit S3 region usage, choose free-tier EC2 instance |

## ⚖️ Trade-Off Decisions

| Area | Options | Decision Basis |
|------|----------|----------------|
| **ETL engine** | Glue vs Polars | Polars for simplicity and control under free tier |
| **Data storage** | Full cloud vs hybrid | Hybrid ensures AWS exposure + cost control |
| **Transformation tool** | dbt vs native code | dbt optional; focus on reproducibility with scripts |
| **Dashboard deployment** | EC2 vs Streamlit Cloud | Streamlit Cloud during dev, EC2 for final demo |
| **Data volume** | Full vs sampled | Sample for testing; scale full later |
| **Automation scope** | Full CI/CD vs scoped | Prioritize lint/test/build integration |

## 🚀 Forward View

| Phase | Focus | Scope |
|--------|--------|--------|
| **During Project (Nov–Dec)** | Build & deploy full batch pipeline | Ingestion → transformation → orchestration → visualization |
| **Next Stage (Post-Project)** | Optimize & automate | dbt lineage docs, CloudFormation templates, role-based access |
| **Future Extensions** | Real-time + ML integration | Kafka streams, feature store, API endpoints for analytics-as-a-service |

## Repository Structure

├── dags/
│ ├── extract_load_dag.py
│ ├── transform_dag.py
│ └── analyze_dag.py
│
├── scripts/
│ ├── ingest_data.py
│ ├── transform_data.py
│ ├── analyze_data.py
│
├── configs/
│ ├── aws_config.yaml
│ └── dbt_project.yml
│
├── tests/
│ ├── test_ingestion.py
│ ├── test_transformations.py
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .github/workflows/ci.yml
├── architecture_diagram.png
└── README.md

## 🌐 Core Data-Engineering Principles Demonstrated

| Principle | Implementation |
|------------|----------------|
| **Scalability** | Modular DAGs, S3-based storage, Polars parallelism |
| **Reproducibility** | Docker + CI/CD ensures identical builds |
| **Observability** | Airflow DAG tracking + CloudWatch logs |
| **Governance** | Schema registry, documentation, versioned data |
| **Security** | IAM roles, private keys via AWS Secrets Manager |
| **Reliability** | Task retries, validation before load |
| **Efficiency** | Partitioned reads, lazy transformations |

---

## 🎯 Expected Outcomes

- Functional **AWS-integrated data engineering pipeline**
- Demonstration of modern DE principles: orchestration, CI/CD, reproducibility
- Analytical insights into **delivery performance, customer behavior, and seller KPIs**
- Repository structured for **scaling to production-ready architecture**

---

## 🏁 References
- [Olist E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  
- [AWS Free Tier Services](https://aws.amazon.com/free/)  
- [Apache Airflow Docs](https://airflow.apache.org/docs/)  
- [Polars Docs](https://pola.rs/)  
- [Great Expectations](https://greatexpectations.io/)  
- [Streamlit](https://streamlit.io/)  
