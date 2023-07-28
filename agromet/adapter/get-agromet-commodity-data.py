import pandas as pd
import json
from amqp import publish

exchange_to_publish = "imd.gov.in/d7733b0c544dfb3137767e5efa2c1ccd3b069bf8/rs.adex.org.in/agromet-imd"
route = "imd.gov.in/d7733b0c544dfb3137767e5efa2c1ccd3b069bf8/rs.adex.org.in/agromet-imd/.commodity-codes"

def agromet_transform(path):
   
    try:
        df = pd.read_csv(path)
        final_df = df.loc[df["lang_id"] == 2] 

        
    except Exception as e:
        print("Error while accessing the file")
        print(e)
    
    id = route.split("agromet-imd/.")[1]
    
    for row in range(0, final_df.shape[0]):
            agromet_dictionary = {
                "id" :   exchange_to_publish +"/"+ id,
                "commodityCode": str(final_df.iloc[row,1]),
                "commodityName":  str(final_df.iloc[row,2]) 
                }
            
            publish(exchange=exchange_to_publish, routing_key=route, message=json.dumps(agromet_dictionary))

    return None

if __name__ == "__main__":
    path = "../misc/crop_tb.csv"
    agromet_transform(path)