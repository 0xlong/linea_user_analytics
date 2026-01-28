# Project Specification: MetaMask-to-Linea "Growth & Retention Engine"

## 1. Context & Objective
I am applying for a **Senior Data Analyst** role at **Consensys**. This role specifically partners with the **MetaMask** product team and sits within the Data team under the Office of the COO. The goal is to build a portfolio project that demonstrates my ability to solve complex Web3 product challenges using the exact stack and strategic mindset requested in the job description.

## 2. The Job Requires
Based on the official job description, a candidate must be able to:

* **Partner with Product Teams**: Work closely with MetaMask product managers, designers, and engineers to anticipate data needs and provide a competitive information advantage.
* **Generate Actionable Insights**: Use exploratory data analysis on complex, high-dimensional datasets—including product event logs, on-chain data, and social data—to influence strategic decisions.
* **Model Data for Truth**: Develop trusted sources of truth by modeling data in **dbt** to measure the success of product initiatives and guide the product roadmap.
* **Scale User Growth**: Uncover insights that help take MetaMask from millions to billions of users by improving user experience and driving growth.
* **Leverage AI Tools**: Contribute to the build-out of the data platform and enhance overall analytics capabilities through the integration of **AI tools**.
* **Communicate with Leadership**: Translate complex findings into well-designed data visualizations and present actionable information to senior leadership.
* **Manage the Data Lifecycle**: Oversee the entire analytics lifecycle, from inception and hypothesis formulation to delivery and ongoing support.
* **Cultivate Data Culture**: Drive the adoption of best practices, set data standards, and optimize processes across the company.
* **Apply Technical Expertise**: Demonstrate high proficiency in **SQL** and **Python**, and utilize data visualization products such as Looker, Tableau, or Mixpanel.
* **Navigate Web3 Complexity**: Apply a demonstrable interest in blockchain and experience working with on-chain data (EVM, SVM, UTXO) and event logging tools like GTM or Segment.

## 3. The Strategic Vision
Consensys is heavily invested in **Linea**. A critical business question is: *How effectively is the MetaMask ecosystem funneling and retaining users within the Linea network?* I want to build a **"Product Strategy Prototype"** that analyzes these user journeys.

## 4. High-Level Requirements
I need you to act as a **Lead Data Architect** to design and code the MVP for this project. Please provide:

### A. The Data Modeling Strategy (SQL)
Design a sophisticated SQL approach (compatible with **Dune Analytics** or standard EVM schemas) to analyze **Cohort Retention**.
* **Acquisition:** Identify users bridging from Ethereum Mainnet to Linea.
* **Segmentation:** Differentiate between "Whales" and "Retail" based on bridge volume.
* **Retention:** Track monthly active behavior on Linea for these specific cohorts over time.



## 5. Technical Constraints & Stack
* **Core Languages:** SQL and Python.
* **Data Tools:** dbt-style modeling logic is preferred.
* **AI Framework:** LangChain, OpenAI API, or Claude API.
* **Output:** A professional deliverable suitable for "Senior Leadership".


## MAYBE IN FUTURE DEVELOPMENT NOT NEEDED NOW
### 4B. The AI Insight Agent (Python) - for future feature developemnt
Develop a Python-based "AI Analyst" using **LangChain** or a direct LLM integration.
* **The Input:** Ingest the retention/cohort data generated in the SQL step.
* **The Task:** Act as a Senior Product Analyst, interpreting raw numbers to generate a "Natural Language Executive Summary".
* **The Goal:** Highlight churn risks, identify the most successful user segments, and suggest one actionable product improvement for the MetaMask/Linea experience.
