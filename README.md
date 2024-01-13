## Effect of Public Health Measured on Flu


Repository Structure
--------------------

    |- Heirarchial Bayesian model attempt.ipynb           # Attempt of Bayesian Model which didn't work
    |- ILINet.csv, Mask API, OxCGRT, SVI2020_US_COUNTY.csv, StateToStateUSA.py, finaldf.csv # Different data sources and ETL scripts
    |- Time Series Model Attempt.ipynb     # Attempt of Time series Model which didn't work
    |- ILI_VIZ.ipynb #  General Data Visualizations
    |- correlation-lags.ipynb # Correlations analysis and visualization
    |- correlation.ipynb  # Correlations analysis and visualization for features lags
    |- flmer-baseline.ipynb  # Linear Mixed effects model baseline
    |- lmer_optimized_model.ipynb        # Linear Mixed effects model Optimized

## Project Goal

    This Project is about finding out the effects of Public health measures such as masking, school restrictive policies, mobility restrictions, etc on flu transmission. We attempt to find out if these health measures are significant in reducing flu transmissions and if so by how much the flu transmission reduces if the public health measure changes.


## Data Processing

We got the data sources from the official CDC website. The target was a variable that indicates transmission of flu and the features used to predict the target were the public policies implemented during COVID.
We used Python, pandas, numpy, and other libraries. to do data processing. Since data was from different sources, we combined the data into a single CSV file and then performed some steps like making sure every column was in a suitable format and aggregating different times scaled to weekly time. Then data visualizations were done to filter out data that did not make sense logically.

## Feature Engineering

There were different mobility variables in our data such as workplace, residential, etc, so we applied PCA to compress these variables to a single variable to reduce collinearity in the data which will affect our model and interpretation. We also performed scaling which improved our model fit.

## Model Fit and Evaluation

We used a linear mixed effects model where the features have a linear relationship with the target. We used this model because we wanted to capture both the fixed effects(public policies ) and random effects (time-varying effects) in the data. And the linear mixed effects model does that.
This model is hierarchical because the fixed effects are common across all states and the random effects are specific to each state. So model here fits each state independently using data for each state and finally produces a single coefficient for each feature for all the states

We used the r2 score as a model evaluation, The r2 score is suitable because we can assess if the public policies can explain the variance present in the flu transmission. 


    
