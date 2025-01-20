import pytest
import HelperFunctions.HelperFunctions as f
import pandas as pd

def test_load_and_clean_data():

    df = f.LoadAndcleanData('test\data\TestData.csv','TestData')
    print(df)
    result=df.loc[df['turbine_id']==1,'power_output'].iloc[0]
    # assert not df['power_output'].isna().any()
    assert result ==2.7,f"Expected 2.7, but got {result}"
def test_computestats():
    df = f.LoadAndcleanData('test\data\TestData.csv','TestData')
    sdf=f.ComputeStats(df,'D','TestData')
    # print(sdf.columns)
    assert 'min_power_output' in sdf.columns
    assert 'max_power_output' in sdf.columns
    assert 'mean_power_output' in sdf.columns
def test_findanamoly():
    df = f.LoadAndcleanData('test\data\TestData.csv','TestData')
    resultdf=f.IdentifyAnamolies(df,'TestData')
    result=df.loc[df['wind_direction']==104,'is_anomaly'].iloc[0]

    assert True==result,f"Expected True, but got {result}"
