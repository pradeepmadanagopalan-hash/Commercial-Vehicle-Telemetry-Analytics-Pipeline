# Commercial-Vehicle-Telemetry-Analytics-Pipeline
⭐ **Introduction**

Modern commercial vehicles generate high-frequency telemetry data that is rich but highly unstructured, noisy, and inconsistent across fleets.

This project builds an end-to-end telemetry analytics pipeline that transforms raw vehicle signals into structured, analysis-ready representations. 
It enables fleet-level comparison, behavioral analysis, and data-driven validation across multiple vehicle groups using scalable PySpark workflows.

---

🧩 **Problem Complexity & Motivation**

Processing commercial vehicle telemetry at scale introduces several challenges:

• **High variability in raw signals** across vehicles, routes, and operating conditions  
• **Noisy and incomplete time-series data**, requiring multi-stage validation and cleaning  
• **Heterogeneous feature distributions**, making direct comparisons across vehicles unreliable  
• **Need for robust aggregation logic** to convert time-series signals into meaningful fleet-level representations  
• **Outlier sensitivity**, where a small number of vehicles can heavily bias fleet statistics  

To address this, the pipeline implements a structured multi-stage framework involving feature engineering, statistical filtering, spectra generation, and validation-based visualization.

---

🎯 **Objectives**: Tha main goals of this project are <br>  

•	Generate load spectra representations from raw telematics time-series data <br> 
•	Compute fleet-level statistical summaries (velocity, weight, gradient, fuel consumption and other derived metrics) <br> 
•	Identify and remove data quality issues and outliers <br> 
•	Provide interpretable visualizations for validation and analysis <br> 
•	Produce curated datasets for downstream analytics and catalog storage <br> 

---

🛠 **Tech Stack** <br> 

•	Apache Spark (PySpark) – distributed data processing <br> 
•	Pandas / NumPy – intermediate transformations and validation <br> 
•	Matplotlib – fleet-level visual analytics <br> 
•	TALPY Time-Series Framework – statistical feature engineering layer <br> 
•	Delta Lake / Databricks Tables – persistent storage layer <br> 

---

🚀 **Pipeline Architecture Overview**

```mermaid
flowchart TD

A[Raw Telemetry Signals Vehicle-Level Data] --> B[TALPY Time-Series Feature Engineering]

B --> C[sgX_talpy_stats Vehicle-Level Aggregated Features]

C --> D[Initial Outlier Detection]

D --> D1[Minimum Mileage Filter]
D --> D2[IQR-based Speed Filtering]
D --> D3[Data Completeness Checks]

D1 --> E[outliervins Unified VIN Exclusion List]
D2 --> E
D3 --> E

C --> F[Spectra Generation]

F --> F1[Velocity Spectra]
F --> F2[Weight Spectra]
F --> F3[Gradient Spectra]

F1 --> G[Binning and Normalization]
F2 --> G
F3 --> G

G --> H[Combined Spectra Tables]

H --> I[Spectra Outlier Removal using outliervins]

I --> J[Clean Spectra Outputs]

J --> J1[Combined Velocity Spectra Clean Stage]
J --> J2[Combined Weight Spectra Clean Stage]
J --> J3[Combined Gradient Spectra Clean Stage]

J2 --> K[Secondary Weight Specific Filtering]

K --> K1[Detect All Zero Weight Distributions]

K1 --> L[weight_spectra_outliers]
J --> M[Visualization and Validation]

M --> M1[Fleet Level Averages]
M --> M2[Per Vehicle Distributions]

M --> M3[Before vs After Comparison]

```
---

📊 **Project Outcomes - Key Numbers**

This pipeline operates at commercial fleet scale and processes large-scale telematics data:

• 🚛 21,000+ commercial trucks analyzed 
• 🧑 300+ individual customers analyzed
• 📡 30+ telemetry signals processed per vehicle  
• 🧠 3 major feature domains (velocity, weight, gradient)  
• 📊 15 powertrain configurations evaluated (SG1–SG15)  
• ⚙️ 400 billion+ time-series records transformed into structured analytics features  
• 📦 11 curated Delta tables generated for downstream analytics  

---

📈 **Sample Visualizations**

---

⚠️ **Data Note**

This project uses proprietary internal datasets from Daimler Truck AG.  
Only the processing pipeline and code structure are shared in this repository for demonstration and learning purposes.  
No raw or sensitive data is included.

---


