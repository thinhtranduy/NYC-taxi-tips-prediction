[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Yi0Zbe2y)
# MAST30034 Project 1 README.md
- Name: `Duy Thinh Tran`
- Student ID: `1369324`

**Research Goal:** This research aim to predict tipping patterns in NYC based on location, by splitting tip-distance-ratio into 3 categories, small, normal and large tips.

**Timeline:** The timeline for the research area is July 2023 to December 2023.
Ensure requirements are met (all requirements for reproducing stored in `requirement.txt`)

Please visit the `scripts` directory and run the files in order to generating raw data:
1. `downloadData.py`: This downloads the raw data into the `data/landing` directory.
2. `weatherDataDownload.py`: This download weather data into `data` directory. 
3. Manually download `Taxi Zone Shapefile` and `Taxi Zone Lookup Table` from (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and stored them in  `data/taxi_zones`


Please visit `notebooks` directory and run these files in order to generate curated data ready for analysis: 

1. `rawDataGenerate.ipynb`: This generates raw data in the `data/raw` directory. 
2. `weatherDataProcessing.ipynb`: This generates hourly weather data in `data` directory.
3. Run `tazi_zones_plot.ipynb` to generates GeoJson file in `data/taxi_zones` directory for geo spatial visualiztion
3. `curatedDataGenerate.ipynb`: This perform necessary processing and analysis on the data, and stored in `data/curated`. For more details, please refer to the content in the notebook.
4. `ExploringDataset.ipynb`: This provides details analysis about features chosen for the models.
5. To train and test the Random Forest model, run the script inside `RandomForest.ipynb`
6. To train and test the Neural Network model, run the script inside `MLP.ipynb`

Plots are stored in `plots` directory.


