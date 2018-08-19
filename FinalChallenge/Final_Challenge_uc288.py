# Final challenge
# Unisse Chua (uc288)

from pyspark import SparkContext, SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
import csv
import sys
from functools import partial
import geopandas as gpd

# Creates an array of phrases from the original tweet
# The array will then be used for the set intersection
def createPhraseList(_, rows):
    import re
    
    separator = re.compile('\W+')
    
    # Setting to 8 because keywords have 8 words max
    n = 8
    for row in rows:
        phrases = []
        fields = row.split('|')
        words = separator.split(fields[5].lower())
        length = len(words)

        for i in range(length):
            for j in range(1, n+1):
                # Stop when the length of tweet is reached 
                if i+j > length:
                    break
                phrases.append(' '.join(words[i:i+j]))
        
        yield ((float(fields[2]), float(fields[1])), phrases)

# Using the phrase list, get only those that match
def filterTweets(bTerms, _, rows):
    terms = bTerms.value

    for row in rows:
        coor, phrase_list = row
        match = terms.intersection(phrase_list)
        
        if len(match) > 0:
            yield coor, match

# Create index function to find the census tract zone
def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd

    zones = gpd.read_file(SparkFiles.get(shapefile)).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

# Finds the census tract where the match is in
# Checks if the geometry is valid first prior to running contains
# Errors encountered with invalid geometries from the geoJSON 
# as per geopandas specifications
def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].is_valid and zones.geometry[idx].contains(p):
            return zones.plctract10[idx]
    return None

def processTweets(pid, records):
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex('500cities_tracts.geojson')
    counts = {}

    for row in records:
        lnglat, tweet = row
        p = geom.Point(proj(lnglat[0], lnglat[1]))
        zone = findZone(p, index, zones)
        if zone:
            counts[zone] = counts.get(zone, 0) + 1
    # returns only the individual counts for the partition
    return counts.items()
       
if __name__=='__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    
    TWEETS_FN = sys.argv[-3]
    DRUGS_FN = sys.argv[-2]
    CITIES_FN = sys.argv[-4]
    
    keywords = set(sc.textFile(DRUGS_FN, use_unicode=False).cache().collect())
    bTerms = sc.broadcast(keywords)

    # loads the geoJSON into a dataframe and gets the tract and population info
    tracts = sqlContext.read.load(CITIES_FN, format="json")
    tracts_df = tracts.select(explode(tracts.features))
    tracts_df = tracts_df.withColumn('plctract10', tracts_df['col.properties.plctract10'])\
	    .withColumn('plctrpop10', tracts_df['col.properties.plctrpop10'])\
	    .select(['plctract10', 'plctrpop10'])

	# retrieves only non-zero population to avoid divide by zero errors
    tracts_df = tracts_df.filter(tracts_df.plctrpop10 > 0)
    
    # convert to RDD for join
    tracts_rdd = tracts_df.rdd.map(tuple)
    
    drugTweets = sc.textFile(TWEETS_FN).mapPartitionsWithIndex(createPhraseList) \
                    .mapPartitionsWithIndex(partial(filterTweets, bTerms))\
                    .mapPartitionsWithIndex(processTweets)\
                    .reduceByKey(lambda x,y: x+y)

    final = drugTweets.join(tracts_rdd)\
    				.mapValues(lambda x: round(x[0] / x[1], 2))\
    				.sortByKey()\
                    .saveAsTextFile(sys.argv[-1])
    # print(sc.textFile(TWEETS_FN).mapPartitionsWithIndex(createPhraseList) \
    #                 .mapPartitionsWithIndex(partial(filterTweets, bTerms))\
    #                 .mapPartitionsWithIndex(partial(processTweets, shapefile)).collect())
    #     