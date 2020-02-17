from Subscripts import Constants
from Subscripts import DataFrameProcessing
import pandas as pd
import numpy as np
import logging
import os
import json


def main():
    # Create Instance of DF Processor
    DFInst = DataFrameProcessing.DataFrameProcessor()
    if os.path.isfile(Constants.MALLFILEPATH) and os.path.isfile(Constants.COMPETITORSFILEPATH):
        ProductOrig = pd.read_csv(Constants.MALLFILEPATH)
        CompetitorsOrig = pd.read_csv(Constants.COMPETITORSFILEPATH)
        # Do median analysis
        MedianAnalysis = DFInst.MedianAnalysis(ProductOrig,CompetitorsOrig)
        with open(Constants.MEDIANANALYSISFILEPATH, 'w') as fp:
            json.dump(MedianAnalysis, fp)
        # Do products analysis
        ProductsAnalysis = DFInst.ProductsAnalysis(ProductOrig,CompetitorsOrig)
        ProductsAnalysis.to_csv(Constants.PRODUCSTANALYSISFILEPATH, index=False)
    else: 
        raise Exception("CSV files werent found")



if __name__ == '__main__':
    main()