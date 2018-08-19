# HW2
# Unisse Chua (uc288)

from pyspark import SparkContext
import csv
import sys

def extractCuisines(partId, list_of_records):
    if partId==0: 
        next(list_of_records) # skipping the first line
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        (cuisine, camis) = (row[7], row[0])
        yield (cuisine, camis)
            
if __name__=='__main__':
    sc = SparkContext()

    YELP_FN = 'nyc_restaurant_inspections.csv'
    
    yelp = sc.textFile(YELP_FN, use_unicode=False).cache()
    
    cuisines = yelp.mapPartitionsWithIndex(extractCuisines)
    
    sc.parallelize(cuisines.distinct().countByKey().items()) \
                    .sortBy(lambda x: -x[1]) \
                    .saveAsTextFile(sys.argv[-1])
