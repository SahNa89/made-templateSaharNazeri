# Project Plan

## Title
<!-- Give your project a short title. -->
Investigating the association between   air pollutant's concentration and temperature changes.

## Main Question

<!-- Think about one main question you want to answer based on the data. -->
1. How the impact of air pollutant concentration is related to weather modification especially temperature changes over 5 years in Ispra, Italy.

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. -->
Air pollution has long been recognized as threatening to human health and global warming. Although emissions of most primary pollutants have declined in Europe, North America, and Japan from the 1990s until the present, air pollution is still a serious problem in many places. It is currently considered to be one of the largest environmental issues in the world.
there datasets are related to Measurements of equivalent black carbon and Measurements of particle number concentration in Ispra, Italy on 2021. These are published by European Commission, Joint Research Centre and are related to atmospheric measurements for emission gasses which can impact each other. With these information we can find out incressing in one of them can have impact on the other or not.


## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->
Datasource1 is for Measurements of particle number concentration in Ispra, Italy on 2021.
Datasource2 is for Measurements of equivalent black carbon in Ispra, Italy on 2021.
these datasets have been downloaded then they are extracted. 
adding metadata and clarify its datatypes then comparison between their measurements based on timeline and for the last part it loaded on sqlite database.

### Datasource1: ExampleSource
* Metadata URL:
 http://data.europa.eu/89h/db31c49c-77a8-48df-abc5-aa999ff6494f
 http://data.europa.eu/89h/f600d5d9-87b5-44ad-9b33-db45524936ae

* Data URL: 
  "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/DMPS_Particle_Concentration_2021.csv"
 "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/Equiv_BlackCarbon_AETH_2021.csv"
* Data Type: CSV
 Metadata URL: https://dev.meteostat.net/bulk/daily.html 
Data URL: "http://bulk.meteostat.net/v2/daily/16066.csv.gz" #station ID:1606.
In addition, There are side data sources essential for understanding the concept of the main dataset. 
Thes documentss typicallyhelpe mappings o column namess to their corresponding meanings in th primaryin data soure 
Data URL: "https://bulk.meteostat.net/v2/stations/full.json.gz" 
and "https://bulk.meteostat.net/v2/daily/16066.map.csv.gz"
## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. What data is available? What errors are there?
2. What limitations present themselves, is data missing and representative? [#2](https://github.com/SahNa89/made-templateSaharNazeri/issues/2)
3. What data types are relevant, do you need to research anything? [#3](https://github.com/SahNa89/made-templateSaharNazeri/issues/3)
4. What is Data exploration and how would data pipeline be performed? [#4](https://github.com/SahNa89/made-templateSaharNazeri/issues/4)
5. 



