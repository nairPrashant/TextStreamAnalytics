# TextStreamAnalytics
Computing statistics for a text stream within a rolling time window

## Input Data
TextStreamAnalytics/StreamProcessing/src/main/resources/data.txt

## Output Data(tab-separated)
TextStreamAnalytics/StreamProcessing/output/out/

## Algorithm
TextStreamAnalytics/StreamProcessing/src/main/scala/com/nair/TextStreamAnalytics.scala

1. Read tab-separated data into spark dataframe
2. Preprocess dataframe: change column types etc.
3. Define window specification: wf
4. Use wf to create aggregates per frame; add these aggregates as new columns to dataframe
5. Output datframe as tab-separated file.
