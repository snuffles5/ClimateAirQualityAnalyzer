# Investigating the Impact of Weather Conditions on Daily Air Quality Levels in Israel

## Introduction

This project aims to analyze the relationship between weather conditions and air quality levels in various locations across Israel. By examining factors such as temperature, humidity, precipitation, and weather conditions, the study will uncover patterns and trends that might help explain how weather fluctuations influence air quality. 

## Motivation

The primary motivation for carrying out this project is a personal passion for environmental protection and a genuine interest in understanding the factors affecting air quality in Israel. The findings from this project may contribute to better understanding of local environmental issues, inform policy-making, and promote public awareness. The academic interest and potential positive impact on the environment make this project both personally and socially meaningful.

## Research Question

The key research question this project aims to answer is: "How do weather conditions affect daily air quality levels in Israel?"

## Dataset

The project will work with at least 50,000 data points, covering 14 different features including but not limited to: Date, Temperature, Wind Speed, Nitrogen, Particulate Matter. 

Data sources for this project include:

- [Israeli Air Monitoring Data](https://air.sviva.gov.il/)
- [Israeli Meteorological Service](https://ims.gov.il/)

Data will be collected via REST-API and web scraping & crawling.

## Data Cleaning

Data cleaning steps will include: removing duplicate entries, handling missing data through imputation or removal, standardizing date and time columns, and identifying and dealing with outliers or erroneous data points.

## Data Exploration and Visualization

Exploratory data analysis (EDA) will be performed using descriptive statistics to summarize the data. Time-series plots will be used to visualize trends in air quality over time. Correlation analysis will be performed between weather variables and the air quality index (AQI).

## Modeling

Models planned for this project include linear or multiple regression to assess the relationship between weather variables and AQI, and Decision Tree Regression to identify important features and their interactions.

## Model Validation

To ensure the validity of the models, the dataset will be split into training and testing sets to evaluate model performance on unseen data. Performance metrics such as R-squared, mean squared error (MSE), and mean absolute error (MAE) will be used to quantify the accuracy of the models. Different models' performance will be compared and the most appropriate one will be selected based on the evaluation metrics and the research question.
