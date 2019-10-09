# Format

## log header format

header of log, one line csv format

Example:
receive time,type,source ip,from port

## file regex

The log file name must contains date, especially for month and day.
Provide the regex pattern to extract the date.

Example:
 - output_11_01.log: `.*_(\d+_\d+).*`
 - output_abnormal_11_29.log: `.*_(\d+_\d+).*`
 - log_12_03: `.*_(\d+_\d+)`

 ## features

The feature name wanted to be extract, MUST in header

 ## excluded data

 The date wanted to be excluded, these data will be the test data, and the remained data will be train data.
