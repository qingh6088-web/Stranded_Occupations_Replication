This document provides step-by-step instructions to reproduce all data processing and analysis results in this project. Please follow the steps in the order listed below.

Step 1. Generate skill similarity and Input-output long-panel data (Python)
1.Run jaccard_sim_calculate.ipynb
Purpose: Calculate skill similarity between occupations using the Jaccard similarity.
Output: “Stranded_Occupations_Replication/use/panel_jskill_sim.csv”

Note: The Python script must be executed before moving to the Stata procedures.

Step 2. Data Preparation (Stata)
2.Run data_preparation.do
Purpose: Clean, merge, and process the datasets.
Output:“Stranded_Occupations_Replication/temp/......”;“Stranded_Occupations_Replication/use/simocc885&851_uq.xlsx”;“Stranded_Occupations_Replication/use/fig2cl.xlsx”;“Stranded_Occupations_Replication/use/fig2cr.xlsx”
Software: Stata 17 or higher.

Step 3. Data Analysis and Visualization
3.Run data_analysis.do (Stata)
Purpose: Conduct analyses and generate the statistical results.
Output: Generate Fig.1C, 2A and Supplementary figure 1.

4.Run fig1.ipynb (Python)
Purpose: Generate Fig.1A and Fig.1B.

5.Run spfig2.ipynb (Python)
Purpose: Generate Supplementary figure 2.

6.Run fig2&spfig3.ipynb (Python)
Purpose: Generate Fig.2B, 2C, 2D and Supplementary figure 3.

7.Run fig3.ipynb (Python)
Purpose: Generate Fig.3.

8.Run fig4&spfig5.ipynb (Python)
Purpose: Generate Fig.4 and Supplementary figure 5.

9.Run fig5&spfig6.ipynb (Python)
Purpose: Generate Fig.5 and Supplementary figure 6.
