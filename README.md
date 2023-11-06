# aquainfra
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





