# aquainfra
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/NIVANorge/niva-aquainfra/HEAD?urlpath=%2Fdoc%2Ftree%2Fcode%2Fplot_ferrybox.ipynb)

Jupyter Notebooks for Oslo Fjord use cases, Aquainfra project 

### General data flow, to be agreed
```mermaid
flowchart TB
  L0[Level_0 raw satellite images] --Correction --> L1(Level_1 corrected images)--Calculation algorithm--> L2[Level_2 Product]

  fb[Ferrybox dataset]
  glomma[Glomma logger dataset]
  insitu_data[In situ measurements dataset]
  fb --> ADC
  glomma  --> ADC
  insitu_data --> ADC

ddos[AquaInfra Data Space]

ADC --Discoverable by --> ddos
L2 -- Limited amount of data uploaded to --> ddos

```

### Use Cases data flow 
##### Example
* Research question: ? 
* Parameters that will be analyzed to answer the question: Chl-a, cDOM (example)
* Jupyter Notebook for this question: ?

```mermaid
flowchart TB

  fb[Query Ferrybox Chla-f, fDOM]
  glomma_dataset[Query glomma fDOM]
  niva_db[Query lab analyzed cDOM abs, Chl-a]
  function[Apply function that makes it possible to compare values across domains]
  analyze[analyze differences?]

  fb --> function
  niva_db --> function
  glomma_dataset --> function

  function --> analyze
  result
 analyze --> result 

``` 

### River data to do list

1. Discharge data (Leah)
  * Share python script for extracting the NVE data
  * Get NVE data for the three rivers 

2. Water chemistry grab samples (Areti)
  * Get from Aquamonitor & Vannmiljø. Same?
  * Make generic, shareable script to clean the data if necessary

3. Glomma sensor data
   * Data retrieved using app and saved (Leah done)
   * Ivana? Make script to access and QC data. Improve on existing QC routines (with Leah/Øyvind K)
   * Leah/Areti: Check sensor data

4. Regressions (Leah/Areti)
  * Concentration vs sensor: FDOM-DOC, Turb-SPM, Conductivity-NO3, ...?
  * Concentration vs discharge
  * Seasonally-variable regressions?

5. Estimate daily concentrations (Leah/Areti)
  - Interpolation
  - Stats relationships from regressions

6. Estimate daily loads (Leah/Areti)
   - Freshwater, DOC, SPM, NO3, TN, TP, ...?
