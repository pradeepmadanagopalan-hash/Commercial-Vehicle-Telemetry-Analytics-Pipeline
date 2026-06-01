# Commercial-Vehicle-Telemetry-Analytics-Pipeline
⭐ **1. Introduction**

Modern commercial vehicles generate high-frequency telemetry data that is rich but highly unstructured, noisy, and inconsistent across fleets.

This project builds an end-to-end telemetry analytics pipeline that transforms raw vehicle signals into structured, analysis-ready representations. 
It enables fleet-level comparison, behavioral analysis, and data-driven validation across multiple vehicle groups using scalable PySpark workflows.

---

🧩 **2. Problem Complexity & Motivation**

This project addresses the challenges of processing commercial vehicle telemetry at scale.

Key challenges include:

- High variability in raw signals across vehicles, routes, and operating conditions  
- Noisy and incomplete time-series data requiring multi-stage validation and cleaning  
- Heterogeneous feature distributions making direct vehicle-to-vehicle comparisons unreliable  
- Need for robust aggregation logic to convert time-series signals into meaningful fleet-level representations  
- Outlier sensitivity where a small number of vehicles can heavily bias fleet statistics  

To address this, the pipeline implements a structured multi-stage framework involving feature engineering, statistical filtering, spectra generation, and validation-based visualization  

---

🎯 **3. Objectives**

This project focuses on building a scalable telemetry analytics pipeline for fleet-level insights.

Key objectives include:

- Generate load spectra representations from raw telematics time-series data  
- Compute fleet-level statistical summaries (velocity, weight, gradient, fuel consumption, and other derived metrics)  
- Identify and remove data quality issues and outliers  
- Provide interpretable visualizations for validation and analysis  
- Produce curated datasets for downstream analytics and catalog storage  

---

🛠 **4. Tech Stack**

This project is implemented using a scalable distributed data processing and analytics stack.

Key technologies include:

- Apache Spark (PySpark) – distributed data processing  
- Pandas / NumPy – intermediate transformations and validation  
- Matplotlib – fleet-level visual analytics  
- TALPY Time-Series Framework – statistical feature engineering layer  
- Delta Lake / Databricks Tables – persistent storage layer

---

🚀 **5. Pipeline Architecture Overview**

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

📈 **6. Sample Visualizations**


**Example 1 (1D Spectra):** Weight Distribution

*Normalized weight spectra distribution across fleet vehicles for a selected powertrain setup (sales group). Users can dynamically adjust bin width and x-axis parameters via a Spark dashboard to generate customized fleet-level distributions.*

<p align="center">
  <img src="Images/1D_Weight_Spectra_Example.png" width="600"/>
</p>

**Example 2 (2D Spectra):** Engine Operation Distribution

*2D engine operation spectra for a selected fleet/powertrain setup (sales group), representing joint distributions of operating parameters. Users can dynamically select both X and Y axes, along with bin widths, via a Spark dashboard to generate customized multi-dimensional fleet-level analyses.*

<p align="center">
  <img src="Images/2D_Engine_Spectra_Example.png" width="600"/>
</p>

**Example 3 (Comparative Analytics):** Fleet Performance Benchmarking

*Multi-dimensional analysis framework allowing users to select multiple operational and performance parameters across different powertrain configurations. The system computes aggregated fleet statistics using mileage- or time-weighted averaging and enables comparative analysis through radar plots to evaluate relative performance across fleets and configurations.*

<p align="center">
  <img src="Images/Operational_Conditions_&_Performance_Analysis_RadarPlotExample.png" width="600"/>
</p>

**Example 4 (Distribution Analysis):** Fuel Consumption Comparison

*Box plots comparing fuel consumption distributions across four user-selected customers. Such visualizations help identify variability, outliers, and operational efficiency differences between fleets, enabling data-driven benchmarking and highlighting inconsistencies in driving behavior or usage patterns.*

<p align="center">
  <img src="Images/Customer_Fleet_FC_Analytics_Example.png" width="600"/>
</p>

NOTE: These examples represent only a subset of the available analytical views. The framework supports dynamic generation of comparable visualizations across diverse processed telemetry signals, powertrain configurations, customer fleets and geographical operating regions.

---

📊 **7. Project Outcomes - Key Numbers**

This pipeline operates at commercial fleet scale and processes large-scale telematics data:

• 🚛 21,000+ commercial trucks analyzed <br>
• 🧑 300+ individual customers analyzed <br>
• ⚙️ 400 billion+ time-series records transformed into structured analytics features <br> 
• 📡 30+ telemetry signals processed per vehicle <br> 
• 📊 15 powertrain configurations evaluated (SG1–SG15)  <br>
• 📦 11 curated Delta tables generated for downstream analytics  

---

🧭 **8. Future Extensions**

Potential next-stage enhancements for this pipeline include:

- Real-time streaming telemetry ingestion and online processing  
- Automated anomaly detection using unsupervised learning models  
- Clustering of fleet behavioral patterns  
- Automated pipeline scheduling and execution

---

⚠️ **9. Data Note**

This project uses proprietary internal datasets from Daimler Truck AG.  
Only the processing pipeline and code structure are shared in this repository for demonstration and learning purposes.  
No raw or sensitive data is included.

---

👨‍💻 **10. Key Takeaways / Skills Demonstrated**

Through this project I demonstrate end-to-end capability in building scalable telemetry analytics pipelines, combining distributed data processing, statistical feature engineering, and fleet-level behavioral analysis.

Key skills showcased include:

- Large-scale PySpark data engineering and pipeline design  
- Time-series feature engineering on high-frequency telemetry data
- Multi-stage statistical outlier detection and data quality validation
- Spectral representation of vehicle operational behavior
- Interactive and comparative data visualization design  



