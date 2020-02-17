from Subscripts import DataFrameProcessing
from Subscripts import Constants
import pandas as pd
import numpy as np
import logging
import os


def main():
    # Create Instance of DF Processor
    DFInst = DataFrameProcessing.DataFrameProcessor()

    # Get Products and upload it into CSV
    if os.path.isfile(Constants.MALLFILEPATH):
        ProductOrig = pd.read_csv(Constants.MALLFILEPATH)
    Products = DFInst.PrepareProduct()
    if os.path.isfile(Constants.MALLFILEPATH):
        Products = pd.concat([ProductOrig,Products])
    Products.drop_duplicates(inplace = True)
    Products.to_csv(Constants.MALLFILEPATH, index=False)
    # Get competitors and upload it into csv
    if os.path.isfile(Constants.COMPETITORSFILEPATH):
        CompetitorsOrig = pd.read_csv(Constants.COMPETITORSFILEPATH)
    Piper = DFInst.PreparePiper()
    Washer = DFInst.PrepareWasher()
    if os.path.isfile(Constants.COMPETITORSFILEPATH):
        CompetitorsDF = pd.concat([CompetitorsOrig,Piper,Washer])
    else:
        CompetitorsDF = pd.concat([Piper,Washer])
    CompetitorsDF.drop_duplicates(inplace = True)
    CompetitorsDF.to_csv(Constants.COMPETITORSFILEPATH, index=False)




if __name__ == '__main__':
    main()