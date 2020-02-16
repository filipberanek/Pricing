from Subscripts import DataFrameProcessing
from Subscripts import Constants
import pandas as pd
import numpy as np



def main():
    # Create Instance of DF Processor
    DFInst = DataFrameProcessing.DataFrameProcessor()

    Products = DFInst.PrepareProduct()
    #piper = DFInst.PreparePiper()
    Washer = DFInst.PrepareWasher()

    """
    #Get piper URL
    washerURL = KebolaInst.GetFiles(Constants.DODAVATELWASHER,Constants.OFFSET,Constants.MAXLISTVALUES)
    washerDF = pd.DataFrame()
    washerStringValue = ''
    test = S3Inst.GetFileContent(washerURL[140]['url'])

    for i,washerVal in enumerate(washerURL):
        washersDFPart = pd.read_csv(StringIO(S3Inst.GetFileContent(washerVal['url'])), sep=",")
        if washerDF.shape[0]>0:
            washerDF = pd.concat([washerDF,washersDFPart])
        else: 
            washerDF = washersDFPart
        print("Made {} out of {}".format(i, len(washerURL)))
    
    washersDFPart = pd.read_csv(StringIO(S3Inst.GetFileContent(washerURL[140]['url'])), sep=",")
    """
    print("test")




if __name__ == '__main__':
    main()