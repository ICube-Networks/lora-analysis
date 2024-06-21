This directory stores all the data extracted from the elastic search server 

# Flows

`distrib_XXX_YYY.parquet` contains the list of packets (timestamp, frame counter) for a flow identified by its devAddr (XXX) and the first frame counter of the flow (YYY). 

> Caution: different flows may have packets with the same frame counter. These packets are just not generated chronologically close.